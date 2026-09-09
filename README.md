# BrandUp.Extensions.Messaging

Библиотека для работы с очередями и стримами сообщений через AWS SDK: Amazon SQS / Yandex Message Queue (очереди) и Amazon Kinesis Data Streams / Yandex Data Streams (стримы).

## Пакеты

| Пакет | Описание |
| --- | --- |
| `BrandUp.Extensions.Messaging.Abstraction` | Интерфейсы и модели. Зависимостей от AWS SDK нет. |
| `BrandUp.Extensions.Messaging.AmazonSqs` | Очереди через AWSSDK.SQS: Amazon SQS, Yandex Message Queue, ElasticMQ. |
| `BrandUp.Extensions.Messaging.AmazonKinesis` | Стримы через AWSSDK.Kinesis: Amazon Kinesis, Yandex Data Streams. |
| `BrandUp.Extensions.Messaging.MongoDB` | Хранилище позиций чтения стримов (чекпоинтов) и аренд шардов в MongoDB. |
| `BrandUp.Extensions.Messaging.Testing` | Фейковая in-memory реализация для тестов. |

## Ключевые контракты

- `IMessagePublisher` — фасад публикации: инжектируется в сервисы приложения, транспорт (очередь или стрим) выбирается по типу сообщения.
- `IMessageQueue<TMessage>` — типизированная очередь: publish / receive / delete / abandon, семантика point-to-point с visibility timeout.
- `IMessageStream<TMessage>` — типизированный стрим: публикация с partition key (порядок в рамках группы).
- `IMessageHandler<TMessage>` — обработчик, вызывается hosted-консьюмером в своём DI-scope; успех — сообщение удаляется, исключение — сообщение вернётся и в итоге уедет в dead-letter.
- `MessagingContext` — типизированный контекст: набор очередей свойствами, привязанный к одному подключению; `EnsureQueuesAsync()` для провижининга.
- `IMessageSerializer` — сериализация; по умолчанию JSON (camelCase).

---

## Быстрый старт (SQS / Yandex Message Queue)

### 1. Описать сообщение

```csharp
[Queue("order-created")]
public class OrderCreated
{
    public Guid OrderId { get; set; }
    public decimal Total { get; set; }
}
```

Логическое имя очереди задаётся атрибутом `[Queue]` или параметром `AddQueue`. Физическое имя складывается из переопределений (`Queues`), префикса/суффикса окружения и суффикса `.fifo`.

### 2. Зарегистрировать в DI

```csharp
services.AddSqsMessaging(options =>
{
    options.ServiceUrl      = "https://message-queue.api.cloud.yandex.net"; // Yandex Message Queue
    options.Region          = "ru-central1";
    options.AccessKeyId     = "...";
    options.SecretAccessKey = "...";
    options.QueueNamePrefix  = "dev-";   // очереди окружения без перечисления каждой
    options.AutoCreateQueues = true;     // создать недостающие очереди при первом обращении
})
.AddQueue<OrderCreated>(configure: settings =>
{
    settings.MaxReceiveCount = 5;        // после 5 неудачных доставок — в dead-letter (order-created-dlq)
})
.AddConsumer<OrderCreated, OrderCreatedHandler>();
```

Для Amazon SQS достаточно указать `Region` — endpoint выводится из региона. Если не задавать `AccessKeyId`/`SecretAccessKey`, используется стандартная цепочка учётных данных AWS SDK (IAM-роль на EC2/ECS/EKS, переменные окружения, профиль).

Правила регистрации простые и жёсткие: один тип сообщения — одна очередь или стрим, один обработчик. Повторная привязка (в том числе в другом транспорте) — исключение на регистрации, а не тихая перезапись.

**Имена очередей.** Физическое имя = `префикс + логическое имя + суффикс`. Если для логического имени задано переопределение `options.Queues["order-created"] = "company-shared-orders"` — оно используется **как есть**, префикс/суффикс к нему не применяются (для того переопределения и нужны — например, чужая общая очередь). Для FIFO суффикс `.fifo` добавляется автоматически.

### 3. Публиковать и обрабатывать

```csharp
// Публикация — через фасад, транспорт выбирается по типу сообщения
public class OrderService(IMessagePublisher publisher)
{
    public Task CreatedAsync(Order order)
        => publisher.PublishAsync(new OrderCreated { OrderId = order.Id, Total = order.Total });
}

// Обработка — hosted-консьюмер сам поллит очередь (long polling) и вызывает обработчик
public class OrderCreatedHandler : IMessageHandler<OrderCreated>
{
    public Task HandleAsync(MessageContext<OrderCreated> context, CancellationToken cancellationToken)
    {
        // context.Message, context.MessageId, context.DeliveryCount, context.EnqueuedAt
        return Task.CompletedTask;
    }
}
```

Семантика — at-least-once: обработчик должен быть идемпотентным. Успешное завершение удаляет сообщение (обработанные сообщения батча удаляются одним `DeleteMessageBatch`); исключение оставляет его в очереди до повторной доставки (visibility timeout) и в итоге — до dead-letter очереди, если настроен `MaxReceiveCount`.

Опции консьюмера (`BatchSize`, `WaitTime`, `MaxConcurrency`) валидируются на старте хоста — ошибка конфигурации роняет запуск, а не крутится в цикле ошибок. Для биндинга из конфигурации именованные опции ищутся по `SqsConsumerOptions.NameFor(typeof(OrderCreated))` — полному имени типа сообщения.

### Батчевая публикация

Когда сообщений много, публиковать их по одному — это по одному сетевому вызову на сообщение. Батчевая перегрузка отправляет их пачками средствами транспорта: `SendMessageBatch` для SQS (10 сообщений за вызов), `PutRecords` для стрима (500 записей). Резать батч по лимитам — забота библиотеки, вызывающему коду достаточно передать список:

```csharp
// Одни опции на всех
await publisher.PublishAsync(orders.Select(o => new OrderCreated { OrderId = o.Id }));

// Или свои опции у каждого сообщения — на стриме это обычно свой partition key
await stream.PublishAsync(
    [.. orders.Select(o => new PublishMessage<OrderEvent>(
        new OrderEvent { OrderId = o.Id },
        new PublishOptions { GroupId = o.Id.ToString() }))]);
```

Что гарантируется, а что нет:

- **Результаты приходят в порядке входного списка** — `results[i]` относится к `messages[i]`.
- **Батч — не транзакция.** Транспорт принимает или отвергает каждое сообщение отдельно; отвергнутое не останавливает остальные. Если что-то не прошло, бросается `BatchPublishException`, и в `FailedIndexes` лежат позиции именно тех сообщений, которые нужно повторить, — остальные уже опубликованы.
- **На FIFO-очереди** такой пропуск ломает порядок группы, поэтому, если порядок важен, повторяйте с первой неудачной позиции.
- **Проверки — до первой отправки.** Некорректное сообщение в середине батча приводит к ошибке, а не к наполовину отправленному батчу; фейк из Testing-пакета ведёт себя так же.

### Ядовитые сообщения

Сообщение, которое не десериализуется в тип очереди — или несёт атрибут `BrandUp-MessageType` с именем другого типа, — в обработчик не попадает. Что с ним делать, задаёт `QueueSettings.PoisonMessageHandling`:

- `Redeliver` (по умолчанию) — сообщение остаётся в очереди, повторяется по visibility timeout и уезжает в dead-letter, когда настроен `MaxReceiveCount`. **Без DLQ оно будет повторяться вечно, а на FIFO — блокировать свою группу**, поэтому либо настройте `MaxReceiveCount`, либо выберите `Delete`.
- `Delete` — залогировать и удалить (содержимое теряется, очередь не встаёт).

### FIFO-очереди

```csharp
.AddQueue<OrderCreated>(configure: settings =>
{
    settings.Fifo = true;                       // физическое имя получит суффикс .fifo
    settings.ContentBasedDeduplication = true;  // дедупликация по хэшу тела
})
```

При публикации порядок гарантируется внутри группы: `PublishAsync(msg, new PublishOptions { GroupId = "user-42" })`. Без явной группы вся очередь работает как одна группа.

Без `ContentBasedDeduplication` и явного `DeduplicationId` библиотека генерирует уникальный id на каждую публикацию — дедупликации при этом не происходит, но публикация работает (SQS без id отверг бы её). Отложенная доставка (`PublishOptions.Delay`) на FIFO не поддерживается самим SQS — используйте `QueueSettings.DeliveryDelay`.

### Ручной приём (без hosted-консьюмера)

```csharp
public class Worker(IMessageQueue<OrderCreated> queue)
{
    public async Task RunAsync(CancellationToken ct)
    {
        // По умолчанию — long polling до 20 секунд: пустой цикл дёшев и не жжёт запросы.
        // Для немедленного возврата передайте waitTime: TimeSpan.Zero.
        var messages = await queue.ReceiveAsync(maxMessages: 10, cancellationToken: ct);
        foreach (var message in messages)
        {
            // ... обработка ...
            await queue.DeleteAsync(message, ct);       // успех
            // await queue.AbandonAsync(message, ct: ct); // вернуть в очередь на повтор
        }
        // Или одним вызовом: await queue.DeleteAsync(messages, ct);
    }
}
```

---

## Контексты мессаджинга и несколько аккаунтов

Регистрация выше описывает одно подключение — один облачный аккаунт. Когда назначений много или аккаунтов несколько, удобнее объявить **контекст мессаджинга**: класс-наследник `MessagingContext`, в котором очереди и стримы описаны свойствами — полный аналог `ObjectStorageContext` из BrandUp.Extensions.ObjectStorage. Тип контекста задаёт и состав назначений, и подключение.

```csharp
public class OrderMessaging : MessagingContext
{
    public IMessageQueue<OrderCreated> Created { get; private set; } = null!;      // имя из [Queue] типа сообщения
    [Queue("orders-cancelled")]
    public IMessageQueue<OrderCancelled> Cancelled { get; private set; } = null!;  // имя из атрибута свойства
}

// Контекст со своим подключением
services.AddSqsMessaging<OrderMessaging>(options => { ... })
    .ConfigureQueue<OrderCreated>(settings => settings.MaxReceiveCount = 5)
    .AddConsumer<OrderCreated, OrderCreatedHandler>();

// Или несколько контекстов на одном именованном подключении (один SQS-клиент)
services.AddSqsMessagingConnection("main", options => { ... });
services.AddSqsMessaging<OrderMessaging>("main");
services.AddSqsMessaging<BillingMessaging>("main");
```

Логическое имя свойства: `[Queue]` на свойстве → `[Queue]` на типе сообщения → имя свойства. Состав валидируется на регистрации (сеттер обязателен, один тип сообщения — одно свойство, привязка типа в другом транспорте — ошибка). Инжектировать можно и весь контекст, и отдельные `IMessageQueue<T>` — это одни и те же экземпляры.

### Очереди и стримы в одном контексте

Свойства могут быть и `IMessageStream<T>` — тогда контекст описывает и очереди, и стримы. Транспорты разные, поэтому контекст регистрируется в каждом: SQS заполняет очереди, Kinesis — стримы. Порядок вызовов не важен.

```csharp
public class OrderMessaging : MessagingContext
{
    public IMessageQueue<OrderCreated> Created { get; private set; } = null!;
    public IMessageStream<OrderEvent> Events { get; private set; } = null!;
}

services.AddSqsMessaging<OrderMessaging>(options => { ... })      // очереди контекста
    .AddConsumer<OrderCreated, OrderCreatedHandler>();

services.AddKinesisMessaging<OrderMessaging>(options => { ... })  // стримы того же контекста
    .AddConsumer<OrderEvent, OrderEventHandler>(options => options.ConsumerGroup = "billing");
```

Если стрим-транспорт не зарегистрирован, контекст не соберётся, и ошибка назовёт и свойство, и нужный вызов — свойство никогда не остаётся `null`. Контекст только со стримами тоже допустим; регистрация транспорта, для которого в контексте нет ни одного свойства, — ошибка регистрации, а не молчаливый no-op. `EnsureQueuesAsync` создаёт только очереди: у стрима есть число шардов и retention, это инфраструктура, а не старт приложения.

```csharp
public class Provisioning(OrderMessaging messaging)
{
    // Создать все очереди контекста (вместе с dead-letter) с настройками из регистрации —
    // провижининг при деплое, не дожидаясь ленивого AutoCreateQueues.
    public Task SetupAsync(CancellationToken ct) => messaging.EnsureQueuesAsync(ct);
}
```

В тестах тот же тип контекста поднимается поверх in-memory шины одним вызовом: `services.AddFakeMessaging<OrderMessaging>()` — заполняются и очереди, и стримы, `EnsureQueuesAsync` — no-op.

### Учётные данные

По умолчанию используются либо статические ключи из опций, либо стандартная цепочка AWS SDK (IAM-роль, переменные окружения, профиль), если ключи не заданы. Когда креды временные — STS, обмен токена на ключи, выдача из vault — регистрируется провайдер:

```csharp
public class StsCredentialsProvider : IMessagingCredentialsProvider
{
    MessagingCredentials current = ...;

    // Читается SDK синхронно при подписи запроса — только отдать кеш, без сетевых вызовов
    public MessagingCredentials GetCurrent() => current;

    // Вызывается библиотекой на старте и по таймеру; обновлять, только если пора
    public async Task RefreshAsync(CancellationToken cancellationToken = default) { ... }
}

services.AddSqsMessaging(options => { ... })
    .UseCredentialsProvider<StsCredentialsProvider>();          // или .UseCredentialsProvider(sp => ...)

services.AddSqsMessagingConnection("archive", options => { ... })
    .UseCredentialsProvider<ArchiveCredentialsProvider>();      // креды принадлежат подключению

services.AddKinesisMessaging(options => { ... })
    .UseCredentialsProvider<StsCredentialsProvider>();
```

Как это работает:

- **Провайдер важнее статических ключей** того же подключения; один провайдер на подключение, повторная регистрация — ошибка.
- **Кеш держится тёплым**: hosted-сервис вызывает `RefreshAsync` на старте (провайдер «прогревается» до первой публикации) и дальше раз в минуту — интервал задаётся вторым параметром `UseCredentialsProvider`. Провайдер сам решает, пора ли обновлять, поэтому это heartbeat, а не период ротации.
- **Ошибка обновления не роняет хост**: она логируется, а следующая попытка идёт по расписанию — имеющиеся креды могут быть ещё действительны.
- **Ротация подхватывается без пересоздания клиента**: SDK перечитывает провайдера, когда истекает `ExpiresUtc` (без него — раз в час).
- **Просроченные креды из кеша** приводят к понятной ошибке с именем провайдера, а не к сообщению SDK про его внутренний цикл обновления.

---

## Стримы (Kinesis / Yandex Data Streams)

Yandex Data Streams совместим с протоколом Amazon Kinesis, поэтому используется тот же пакет — меняется только endpoint. Физическое имя стрима в Yandex — полный путь, его удобно задавать через переопределение:

```csharp
services.AddKinesisMessaging(options =>
{
    options.ServiceUrl      = "https://yds.serverless.yandexcloud.net"; // Yandex Data Streams
    options.Region          = "ru-central1";
    options.AccessKeyId     = "...";
    options.SecretAccessKey = "...";
    options.Streams["order-events"] = "/ru-central1/b1g.../etn.../order-events";
})
.AddStream<OrderCreated>("order-events");
```

Публикация — тем же `IMessagePublisher`; `PublishOptions.GroupId` становится partition key (записи одной группы попадают в один шард и сохраняют порядок). Очереди и стримы можно смешивать в одном приложении и даже в одном [контексте](#очереди-и-стримы-в-одном-контексте): каждый тип сообщения привязан к своему транспорту. Опции, которые стрим выполнить не может (`Delay`, `DeduplicationId`), приводят к ошибке, а не игнорируются молча.

В тестах стрим подменяется так же, как очередь: `services.AddFakeMessaging().AddStream<OrderEvent>()` даёт `FakeMessageStream` — публикации видны в `InMemoryMessageBus`, `GroupId` проверяется по тем же правилам, что и в проде, а `DispatchPendingAsync` доставляет записи в обработчик, как это делает hosted-ридер.

### Чтение стримов и чекпоинты

Стрим, в отличие от очереди, **не хранит позицию читателя** — её хранит сам читатель, иначе после рестарта он начнёт с начала. Позиция (sequence number последней обработанной записи) сохраняется через `ICheckpointStore` на каждую пару «шард + группа читателей»:

```csharp
services.AddMongoMessagingCheckpoints();   // IMongoDatabase берётся из DI; или options.DatabaseAccessor

services.AddKinesisMessaging(options => { ... })
    .AddStream<OrderEvent>("order-events")
    .AddConsumer<OrderEvent, OrderEventHandler>(options =>
    {
        options.ConsumerGroup = "billing";              // своя позиция у каждой группы
        options.StartPosition = StreamStartPosition.Oldest;  // с чего начать, если чекпоинта ещё нет
    });
```

Обработчик — тот же `IMessageHandler<T>`, что и для очередей; в `MessageContext` придут `MessageId` (sequence number), `GroupId` (partition key) и `QueueName` (имя стрима).

Как это работает:

- **Каждый шард читается одним циклом**, записи отдаются обработчику по одной — так сохраняется порядок внутри шарда (то есть внутри partition key). Разные шарды читаются параллельно.
- **Чекпоинт пишется после обработки** записи — семантика at-least-once, обработчик должен быть идемпотентным (как и на очередях).
- **Ошибка обработчика** — запись повторяется на месте, шард ждёт (порядок важнее скорости). После `MaxDeliveryAttempts` (по умолчанию 5) запись логируется как critical и пропускается, чтобы одна «ядовитая» запись не остановила шард навсегда; `MaxDeliveryAttempts = 0` — повторять бесконечно.
- **Решардинг** отслеживается: закрытый шард помечается завершённым, дочерние стартуют только после того, как родительский дочитан.
- **Один читатель на шард.** Без хранилища аренд (см. ниже) это соглашение, а не гарантия: запускайте один инстанс на группу читателей, либо задавайте каждому инстансу свою `ConsumerGroup`, если каждый должен видеть все записи.

Хранилище чекпоинтов — одна маленькая запись на шард в коллекции `brandup.messaging.checkpoints` (`_id = stream|group|shard`), обновление одним upsert; имя коллекции меняется через `MongoCheckpointOptions.CollectionName`. В тестах доступен `services.AddInMemoryMessagingCheckpoints()` с инспектируемым `InMemoryCheckpointStore`.

### Несколько инстансов на группу: аренда шардов

Чтобы несколько инстансов одной группы читали стрим вместе, зарегистрируйте `IShardLeaseStore` — тогда каждый шард арендуется одним инстансом, а группа делит стрим между собой:

```csharp
services.AddMongoMessagingCheckpoints();
services.AddMongoMessagingShardLeases();   // без этого группу должен читать один инстанс

services.AddKinesisMessaging(options => { ... })
    .AddConsumer<OrderEvent, OrderEventHandler>(options =>
    {
        options.ConsumerGroup      = "billing";
        options.LeaseDuration      = TimeSpan.FromSeconds(30);   // сколько шард не читается после падения инстанса
        options.LeaseRenewInterval = TimeSpan.FromSeconds(10);   // должен быть меньше LeaseDuration
    });
```

Как это работает:

- **Аренда берётся перед чтением** шарда и продлевается, пока он читается. Инстанс, который упал или потерял сеть, перестаёт продлевать — через `LeaseDuration` шард забирает другой и продолжает с чекпоинта. При штатной остановке аренда отдаётся сразу, ждать истечения не нужно.
- **Доля от стрима** — шарды, делённые на число читателей, с округлением вверх: 5 шардов на 2 инстанса — 3 и 2. Сверх своей доли инстанс не берёт шарды, даже свободные.
- **Передача шарда** запрашивается, только если свободных шардов не хватает: перегруженному читателю ставится отметка, он видит её на очередном продлении и отдаёт шард **после текущей записи** — работа не прерывается на середине. По одному шарду за раз, пока запрошенный не получен.
- **Токен аренды** меняется при каждом взятии, поэтому подвисший читатель не может продлить или удалить аренду, которая уже перешла к другому.
- Аренда **ограничивает дублирование, но не убирает его**: читатель, застрявший дольше `LeaseDuration`, потеряет шард, продолжая обрабатывать запись. Семантика остаётся at-least-once, обработчик должен быть идемпотентным.

Аренды лежат в коллекции `brandup.messaging.leases` (`_id = stream|group|shard`, имя меняется через `MongoShardLeaseOptions.CollectionName`), запись удаляется, когда шард отдан. В тестах — `services.AddInMemoryMessagingShardLeases()` с инспектируемым `InMemoryShardLeaseStore`.

---

## Тестирование

Пакет `BrandUp.Extensions.Messaging.Testing` подменяет транспорт in-memory шиной — тот же `IMessagePublisher` и типизированные очереди, что и в проде. Фейк ведёт себя как продовый транспорт: те же правила имён и регистрации, те же лимиты `ReceiveAsync`, атрибуты сообщений доставляются в обработчик. Не эмулируются только время: `PublishOptions.Delay` игнорируется (сообщения доступны сразу), visibility timeout отсутствует — принятое сообщение просто вне очереди до `DeleteAsync`/`AbandonAsync`.

```csharp
services.AddFakeMessaging()
    .AddQueue<OrderCreated>()
    .AddHandler<OrderCreated, OrderCreatedHandler>();

var bus = provider.GetRequiredService<InMemoryMessageBus>();

// Проверка публикаций
await orderService.CreatedAsync(order);
var published = Assert.Single(bus.PublishedOf<OrderCreated>());

// Доставка в обработчики — как это сделал бы hosted-консьюмер
await bus.DispatchPendingAsync(provider);
```

---

## Архитектура

```text
IMessagePublisher (фасад, роутинг по типу сообщения)
        │
        ├─ IMessageSender<TMessage> ──┬─ IMessageQueue<TMessage>   (AmazonSqs, Testing)
        │                             └─ IMessageStream<TMessage>  (AmazonKinesis)
        │
IMessageHandler<TMessage> ◄─┬─ SqsConsumerService<TMessage>      (long polling, батч-удаление)
                            └─ KinesisConsumerService<TMessage>  (шарды + ICheckpointStore)
```

- Разрешение имён: переопределение из options — точное физическое имя; без него — `префикс + логическое имя + суффикс`; для FIFO добавляется `.fifo`. Лимиты SQS (80 символов, алфавит) проверяются при разрешении.
- Полезная нагрузка — JSON-тело; имя типа сообщения едет в атрибуте `BrandUp-MessageType` и проверяется при приёме — чужой тип не попадёт в обработчик как «пустой» объект.
- Автосоздание очередей (`AutoCreateQueues`) применяет `QueueSettings`, включая создание dead-letter очереди и redrive policy.
- Общий AWS-бутстрап (endpoint, учётные данные, валидация) и правила регистрации разделены между провайдерами как shared-исходники (`src/Shared`), чтобы SQS и Kinesis не расходились в поведении.

## Интеграционные тесты

Гоняются против настоящих серверов — аналогично MinIO в BrandUp.Extensions.ObjectStorage. Без соответствующей переменной окружения тесты помечаются как skipped, поэтому локально набор остаётся зелёным без контейнеров:

| Сервер | Что проверяет | Переменная |
| --- | --- | --- |
| [ElasticMQ](https://github.com/softwaremill/elasticmq) | Очереди SQS | `SQS_SERVICE_URL` |
| [LocalStack](https://github.com/localstack/localstack) (community, `:3.8`) | Чтение стримов Kinesis | `KINESIS_SERVICE_URL` |
| MongoDB | Хранилище чекпоинтов | `MONGO_CONNECTION_STRING` |

```powershell
docker run -d --name elasticmq -p 9324:9324 softwaremill/elasticmq-native
docker run -d --name localstack -p 4566:4566 -e SERVICES=kinesis localstack/localstack:3.8
docker run -d --name mongo -p 27017:27017 mongo:7

$env:SQS_SERVICE_URL = "http://localhost:9324"
$env:KINESIS_SERVICE_URL = "http://localhost:4566"
$env:MONGO_CONNECTION_STRING = "mongodb://localhost:27017"
dotnet test
```

## Планы

- Мост с outbox из BrandUp.Core: публикация доменных событий через `IMessagePublisher`.
