<#
.SYNOPSIS
    Starts or stops the service emulators the integration tests run against, as native Windows processes.

.DESCRIPTION
    The agents of the Test pool run Windows without Docker, so the emulators run as plain processes:
    ElasticMQ for SQS and kinesis-mock for Kinesis on a portable JDK (LocalStack needs Docker), and the
    MongoDB server for the checkpoint and lease stores. Downloads are pinned by version and SHA-256 and cached
    in the tools directory, which a pipeline agent keeps between runs.

    Written for Windows PowerShell 5.1, which every Windows agent has.

.EXAMPLE
    ci/integration-services.ps1 -Action Start
    $env:SQS_SERVICE_URL = 'http://localhost:9324'
    $env:KINESIS_SERVICE_URL = 'http://localhost:4566'
    $env:MONGO_CONNECTION_STRING = 'mongodb://127.0.0.1:27117'
    dotnet test -c Release --filter-trait Category=Integration --ignore-exit-code 8
    ci/integration-services.ps1 -Action Stop
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('Start', 'Stop')]
    [string] $Action,

    # Downloads are cached here; in a pipeline pass $(Agent.ToolsDirectory).
    [string] $ToolsDirectory = (Join-Path ([Environment]::GetFolderPath('LocalApplicationData')) 'BrandUp\ci-tools'),

    # Process ids, logs and data of the running services; in a pipeline pass a folder under $(Agent.TempDirectory).
    [string] $StateDirectory = (Join-Path ([IO.Path]::GetTempPath()) 'BrandUp.Extensions.Messaging.services')
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
Add-Type -AssemblyName System.IO.Compression.FileSystem

$Tools = @{
    Jdk         = @{
        Name    = 'microsoft-jdk'
        Version = '21.0.12.1'
        Url     = 'https://aka.ms/download-jdk/microsoft-jdk-21.0.12.1-windows-x64.zip'
        Sha256  = '192441A9D27DA813BADA974BB88B4CF64D37A9589ED37F204374D411CA5CE07F'
    }
    ElasticMq   = @{
        Name    = 'elasticmq'
        Version = '1.7.1'
        Url     = 'https://github.com/softwaremill/elasticmq/releases/download/v1.7.1/elasticmq-server-all-1.7.1.jar'
        Sha256  = 'A40DFD03FD8E2F17418F3C61A460C1DAEA902119145BDCE97D09A81F03AA0428'
    }
    KinesisMock = @{
        Name    = 'kinesis-mock'
        Version = '0.6.2'
        Url     = 'https://github.com/etspaceman/kinesis-mock/releases/download/v0.6.2/kinesis-mock.jar'
        Sha256  = '66643A9BC261C1873CC9E4EA7B0694B3E42B83ECBA7C90EBF04EB006FE53BC92'
    }
    MongoDB     = @{
        Name    = 'mongodb'
        Version = '7.0.43'
        Url     = 'https://fastdl.mongodb.org/windows/mongodb-windows-x86_64-7.0.43.zip'
        Sha256  = '09B7893DC07FBB6D67D4C2690F6D1FA5B3039A0B3B5DE34FB1D9096D55A2230F'
        # The archive is mostly debug symbols; the server is all the tests need.
        Extract = @('*/bin/mongod.exe')
    }
}

$SqsPort = 9324
$SqsStatsPort = 9325
$KinesisPort = 4566
$KinesisTlsPort = 4567
# Off the default 27017, where an agent machine may run a MongoDB of its own.
$MongoPort = 27117

. (Join-Path $PSScriptRoot 'service-helpers.ps1')

if ($Action -eq 'Stop') {
    Stop-ServiceProcesses
    return
}

Stop-ServiceProcesses
# Exists however early the start fails, so the pipeline always has a folder of logs to publish.
New-Item $LogDirectory -ItemType Directory -Force | Out-Null
# Before the folders are cleared, so a service left by a run whose process list is lost no longer holds their files.
$SqsPort, $SqsStatsPort, $KinesisPort, $KinesisTlsPort, $MongoPort | ForEach-Object { Assert-PortFree $_ }

New-ServiceDirectory 'logs' | Out-Null
Start-Transcript (Join-Path $LogDirectory 'start.log') | Out-Null
try {
    $java = Get-ToolFile (Install-Tool $Tools.Jdk) 'java.exe'
    $elasticMq = Get-ToolFile (Install-Tool $Tools.ElasticMq) '*.jar'
    $kinesisMock = Get-ToolFile (Install-Tool $Tools.KinesisMock) '*.jar'
    $mongod = Get-ToolFile (Install-Tool $Tools.MongoDB) 'mongod.exe'

    # mongod needs the Visual C++ runtime, which a clean agent lacks and the archive only brings as an installer
    # that wants administrator rights. The JDK carries the same runtime, and a DLL next to the executable is
    # loaded before the system one.
    foreach ($dll in 'msvcp140.dll', 'vcruntime140.dll', 'vcruntime140_1.dll') {
        $target = Join-Path (Split-Path $mongod) $dll
        if (-not (Test-Path $target)) { Copy-Item (Join-Path (Split-Path $java) $dll) $target }
    }

    Start-ServiceProcess 'elasticmq' $java @('-jar', $elasticMq)

    Start-ServiceProcess 'kinesis-mock' $java @('-jar', $kinesisMock) @{
        KINESIS_MOCK_PLAIN_PORT = "$KinesisPort"
        KINESIS_MOCK_TLS_PORT   = "$KinesisTlsPort"
    }

    $mongoData = New-ServiceDirectory 'mongodb-data'
    Start-ServiceProcess 'mongodb' $mongod @('--dbpath', $mongoData, '--port', "$MongoPort", '--bind_ip', '127.0.0.1')

    Wait-ServiceReady 'elasticmq' { Test-HttpResponds "http://localhost:$SqsPort/" }
    Wait-ServiceReady 'kinesis-mock' { Test-HttpResponds "http://localhost:$KinesisPort/" }
    # A native mongod accepts connections only once it is ready to serve them.
    Wait-ServiceReady 'mongodb' { Test-TcpListens $MongoPort }
}
finally {
    Stop-Transcript | Out-Null
}
