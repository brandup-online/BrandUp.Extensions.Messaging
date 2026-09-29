# Helpers for ci/integration-services.ps1: downloading pinned tools and running services as background
# processes. Dot-sourced; expects $ToolsDirectory and $StateDirectory from the calling script.

$ProcessListFile = Join-Path $StateDirectory 'processes.txt'
$LogDirectory = Join-Path $StateDirectory 'logs'

# Downloads a file, resuming where an earlier attempt stopped: the server archives are hundreds of megabytes
# and a dropped connection must not start them over. curl.exe ships with Windows 10 and Server 2019; without
# it the download starts over on every attempt.
function Invoke-Download([string] $Url, [string] $File) {
    $curl = Get-Command curl.exe -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
    for ($attempt = 1; ; $attempt++) {
        if ($curl) {
            # A transfer slower than 10 KB/s for a minute is dropped and resumed rather than waited on.
            & $curl.Source --fail --silent --show-error --location --connect-timeout 30 `
                --speed-limit 10240 --speed-time 60 --continue-at - --output $File $Url
            if ($LASTEXITCODE -eq 0) { return }
            # 33: the server does not resume, so the partial file is useless.
            if ($LASTEXITCODE -eq 33) { Remove-Item $File -ErrorAction SilentlyContinue }
            $failure = "curl exit code $LASTEXITCODE"
        }
        else {
            try {
                Invoke-WebRequest $Url -OutFile $File -UseBasicParsing
                return
            }
            catch {
                $failure = $_.Exception.Message
            }
        }

        if ($attempt -ge 8) { throw "downloading $Url failed: $failure" }
        Write-Host "download interrupted ($failure), resuming"
        Start-Sleep -Seconds (5 * $attempt)
    }
}

# Downloads a tool once into $ToolsDirectory\<name>\<version>, verifies its SHA-256 and extracts a zip.
# The folder is filled under a temporary name and renamed at the end, so an interrupted run is never
# mistaken for a complete one. The download itself is kept apart, so a later run resumes it.
function Install-Tool([hashtable] $Tool) {
    $directory = Join-Path $ToolsDirectory "$($Tool.Name)\$($Tool.Version)"
    if (Test-Path (Join-Path $directory '.complete')) {
        return $directory
    }

    $staging = "$directory.partial"
    if (Test-Path $staging) { Remove-Item $staging -Recurse -Force }
    New-Item $staging -ItemType Directory -Force | Out-Null

    $downloads = Join-Path $ToolsDirectory 'downloads'
    New-Item $downloads -ItemType Directory -Force | Out-Null
    $file = Join-Path $downloads "$($Tool.Name)-$($Tool.Version)-$([IO.Path]::GetFileName(([Uri] $Tool.Url).AbsolutePath))"

    Write-Host "downloading $($Tool.Name) $($Tool.Version)"
    Invoke-Download $Tool.Url $file

    $hash = (Get-FileHash $file -Algorithm SHA256).Hash
    if ($hash -ne $Tool.Sha256) {
        Remove-Item $file
        throw "$($Tool.Name) $($Tool.Version): SHA-256 $hash does not match the pinned $($Tool.Sha256)"
    }

    if ($file.EndsWith('.zip')) {
        $archive = [IO.Compression.ZipFile]::OpenRead($file)
        try {
            foreach ($entry in $archive.Entries) {
                if (-not $entry.Name) { continue }
                if ($Tool.Extract -and -not ($Tool.Extract | Where-Object { $entry.FullName -like $_ })) { continue }
                $target = Join-Path $staging $entry.FullName
                New-Item (Split-Path $target) -ItemType Directory -Force | Out-Null
                [IO.Compression.ZipFileExtensions]::ExtractToFile($entry, $target, $true)
            }
        }
        finally {
            $archive.Dispose()
        }
        Remove-Item $file
    }
    else {
        Move-Item $file (Join-Path $staging ([IO.Path]::GetFileName(([Uri] $Tool.Url).AbsolutePath)))
    }

    New-Item (Join-Path $staging '.complete') -ItemType File | Out-Null
    if (Test-Path $directory) { Remove-Item $directory -Recurse -Force }
    Move-Item $staging $directory
    return $directory
}

function Get-ToolFile([string] $Directory, [string] $Filter) {
    $file = Get-ChildItem $Directory -Recurse -File -Filter $Filter | Select-Object -First 1
    if (-not $file) { throw "no $Filter under $Directory" }
    return $file.FullName
}

# A service left running by an interrupted run of this script is stopped; any other listener is an error,
# since the tests would talk to it instead of the emulator.
function Assert-PortFree([int] $Port) {
    $listener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $listener) { return }

    $owner = Get-Process -Id $listener.OwningProcess -ErrorAction SilentlyContinue
    $path = if ($owner) { $owner.Path } else { $null }
    if ($path -and $path.StartsWith($ToolsDirectory, [StringComparison]::OrdinalIgnoreCase)) {
        Write-Host "stopping $($owner.ProcessName) ($($owner.Id)) left on port $Port by an earlier run"
        Stop-Process -Id $owner.Id -Force
        $owner.WaitForExit(10000) | Out-Null
        return
    }

    throw "port $Port is taken by $(if ($owner) { "$($owner.ProcessName) ($($owner.Id))" } else { "process $($listener.OwningProcess)" })"
}

# A fresh directory under the state folder: whatever an earlier run left there is dropped.
function New-ServiceDirectory([string] $Name) {
    $path = Join-Path $StateDirectory $Name
    if (Test-Path $path) { Remove-Item $path -Recurse -Force }
    New-Item $path -ItemType Directory | Out-Null
    return $path
}

# Start-Process with redirection creates the service with handle inheritance on, so it would also inherit every
# inheritable handle of this PowerShell, among them the pipes a pipeline agent reads the step output from: the
# step would then not end until the service exits. Without redirection Start-Process goes through ShellExecute,
# which passes no handles on, so the output is written to files by cmd instead. The recorded process is that cmd,
# with the service as its child.
function Start-ServiceProcess([string] $Name, [string] $FilePath, [string[]] $Arguments, [hashtable] $Environment = @{}) {
    # Doubled trailing backslashes keep a quoted argument from escaping its closing quote.
    $command = (@($FilePath) + $Arguments | ForEach-Object { '"' + ($_ -replace '(\\+)$', '$1$1') + '"' }) -join ' '
    $output = Join-Path $LogDirectory "$Name.log"
    $errors = Join-Path $LogDirectory "$Name.err.log"

    foreach ($key in $Environment.Keys) { Set-Item "env:$key" $Environment[$key] }
    try {
        $process = Start-Process cmd.exe -ArgumentList "/d /s /c `"$command > `"$output`" 2> `"$errors`"`"" `
            -WindowStyle Hidden -PassThru
    }
    finally {
        foreach ($key in $Environment.Keys) { Remove-Item "env:$key" }
    }

    Add-Content $ProcessListFile "$($process.Id) $($process.StartTime.ToFileTimeUtc()) $Name"
    Write-Host "started $Name ($($process.Id))"
}

# The process recorded by a line of the process list, if it still runs. A process id is reused once its process
# exits, so the start time is compared as well: stopping by id alone could kill an unrelated process.
function Get-ServiceProcess([string] $Line) {
    $id, $started, $null = $Line.Split(' ', 3)
    $process = Get-Process -Id $id -ErrorAction SilentlyContinue
    $startTime = if ($process) { try { $process.StartTime.ToFileTimeUtc() } catch { $null } }
    if ($null -ne $startTime -and $startTime -eq ($started -as [long])) { return $process }
}

function Test-HttpResponds([string] $Url) {
    try {
        Invoke-WebRequest $Url -UseBasicParsing -TimeoutSec 5 | Out-Null
        return $true
    }
    catch {
        # Any HTTP answer, an error status included, means the server is up; a refused connection has none.
        return $null -ne $_.Exception.Response
    }
}

function Test-TcpListens([int] $Port) {
    $client = New-Object Net.Sockets.TcpClient
    try {
        $client.Connect('127.0.0.1', $Port)
        return $true
    }
    catch {
        return $false
    }
    finally {
        $client.Dispose()
    }
}

function Write-ServiceLog([string] $Name) {
    foreach ($log in "$Name.log", "$Name.err.log") {
        $path = Join-Path $LogDirectory $log
        if (Test-Path $path) {
            Write-Host "----- $log"
            Get-Content $path -Tail 40 | Write-Host
        }
    }
}

function Wait-ServiceReady([string] $Name, [scriptblock] $Probe, [int] $TimeoutSeconds = 120) {
    $line = Get-Content $ProcessListFile | Where-Object { $_.EndsWith(" $Name") } | Select-Object -Last 1
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    Write-Host "waiting for $Name"
    while ((Get-Date) -lt $deadline) {
        if (-not (Get-ServiceProcess $line)) {
            Write-ServiceLog $Name
            throw "$Name exited before it became ready"
        }
        # A probe that throws, such as a refused connection, means the service is not ready yet.
        $ready = try { & $Probe } catch { $false }
        if ($ready) {
            Write-Host "$Name is ready"
            return
        }
        Start-Sleep -Seconds 1
    }
    Write-ServiceLog $Name
    throw "$Name did not become ready in $TimeoutSeconds seconds"
}

function Stop-ServiceProcesses {
    if (-not (Test-Path $ProcessListFile)) { return }

    foreach ($line in Get-Content $ProcessListFile) {
        $process = Get-ServiceProcess $line
        if ($process) {
            Write-Host "stopping $($line.Split(' ', 3)[2]) ($($process.Id))"
            # The whole tree: the recorded process is the cmd that runs the service. The service is waited for too,
            # as it lets go of its files only once it has exited.
            $children = Get-CimInstance Win32_Process -Filter "ParentProcessId = $($process.Id)" |
                ForEach-Object { Get-Process -Id $_.ProcessId -ErrorAction SilentlyContinue }
            & taskkill.exe /pid $process.Id /t /f | Out-Null
            foreach ($exiting in @($process) + @($children)) {
                if ($exiting) { $exiting.WaitForExit(10000) | Out-Null }
            }
        }
    }
    Remove-Item $ProcessListFile
}
