param(
    [int]$SubscriberCount = 100000,
    [string]$BaseUrl = "http://localhost:8080",
    [int]$TimeoutSeconds = 180
)

$ErrorActionPreference = "Stop"
$runId = Get-Date -Format "yyyyMMddHHmmssfff"
$emailPrefix = "loadtest-100k-$runId"

function Invoke-Psql {
    param(
        [Parameter(Mandatory = $true)][string]$Sql,
        [switch]$TuplesOnly
    )

    $dockerArgs = @(
        "compose", "exec", "-T", "postgres", "psql",
        "-v", "ON_ERROR_STOP=1", "-U", "notification", "-d", "notification_system"
    )
    $dockerArgs += if ($TuplesOnly) { "-tAc" } else { "-c" }
    $dockerArgs += $Sql
    $result = & docker @dockerArgs
    if ($LASTEXITCODE -ne 0) {
        throw "psql failed: $Sql"
    }
    return (($result -join "`n").Trim())
}

function Invoke-JsonPost {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][hashtable]$Body
    )

    Invoke-RestMethod `
        -Method Post `
        -Uri "$BaseUrl$Path" `
        -ContentType "application/json" `
        -Body ($Body | ConvertTo-Json -Compress)
}

$health = Invoke-RestMethod -Uri "$BaseUrl/actuator/health"
if ($health.status -ne "UP") {
    throw "Application is not healthy: $($health.status)"
}

Write-Host "=== Current fan-out SLA test: subscribers=$SubscriberCount runId=$runId ==="

$author = Invoke-JsonPost -Path "/api/users" -Body @{
    email = "$emailPrefix-author@test.local"
    name = "load-test-author"
    notificationChannel = "PUSH"
}
$authorId = [long]$author.id

$seedWatch = [System.Diagnostics.Stopwatch]::StartNew()
Invoke-Psql -Sql @"
INSERT INTO users (email, name, notification_channel)
SELECT '$emailPrefix-sub-' || n || '@test.local', 'load-test-subscriber-' || n, 'PUSH'
FROM generate_series(1, $SubscriberCount) AS n;

INSERT INTO subscription.subscriptions (user_id, author_id, status)
SELECT id, $authorId, 'ACTIVE'
FROM users
WHERE email LIKE '$emailPrefix-sub-%';

INSERT INTO notification.subscriber_read_model (author_id, user_id)
SELECT $authorId, id
FROM users
WHERE email LIKE '$emailPrefix-sub-%';
"@ | Out-Null
$seedWatch.Stop()

$membership = Invoke-Psql -TuplesOnly -Sql @"
SELECT
  (SELECT count(*) FROM subscription.subscriptions WHERE author_id = $authorId AND status = 'ACTIVE') || '|' ||
  (SELECT count(*) FROM notification.subscriber_read_model WHERE author_id = $authorId);
"@
if ($membership -ne "$SubscriberCount|$SubscriberCount") {
    throw "Membership seed mismatch: $membership"
}

$post = Invoke-JsonPost -Path "/api/posts" -Body @{
    authorId = $authorId
    title = "100k fan-out SLA $runId"
    content = "Current Dispatcher/Chunk Worker load test"
}
$postId = [long]$post.id

$publishWatch = [System.Diagnostics.Stopwatch]::StartNew()
Invoke-JsonPost -Path "/api/posts/$postId/publish" -Body @{} | Out-Null
$publishWatch.Stop()

$eventId = ""
$eventDeadline = [DateTime]::UtcNow.AddSeconds(10)
while ([DateTime]::UtcNow -lt $eventDeadline -and -not $eventId) {
    $eventId = Invoke-Psql -TuplesOnly -Sql "SELECT payload->>'eventId' FROM post.outbox_events WHERE aggregate_id = $postId AND event_type = 'PostPublished' ORDER BY created_at DESC LIMIT 1;"
    if (-not $eventId) { Start-Sleep -Milliseconds 50 }
}
if (-not $eventId) { throw "PostPublished outbox event was not found" }

$deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
$lastProgress = ""
do {
    $progress = Invoke-Psql -TuplesOnly -Sql @"
SELECT
  COALESCE((SELECT status FROM notification.fanout_dispatches WHERE event_id = '$eventId'), 'NOT_STARTED') || '|' ||
  (SELECT count(*) FROM notification.notifications WHERE source_event_id = '$eventId') || '|' ||
  (SELECT count(*) FROM notification.notification_delivery_log dl
    JOIN notification.notifications n ON n.id = dl.notification_id
    WHERE n.source_event_id = '$eventId');
"@
    if ($progress -ne $lastProgress) {
        Write-Host "progress=$progress"
        $lastProgress = $progress
    }
    $parts = $progress.Split('|')
    $complete = $parts[0] -eq "DONE" -and [long]$parts[1] -eq $SubscriberCount -and [long]$parts[2] -eq $SubscriberCount
    if (-not $complete) { Start-Sleep -Milliseconds 100 }
} while (-not $complete -and [DateTime]::UtcNow -lt $deadline)

$timing = Invoke-Psql -TuplesOnly -Sql @"
SELECT
  round(extract(epoch FROM (fd.updated_at - oe.created_at)) * 1000, 1) || '|' ||
  round(extract(epoch FROM (max(n.created_at) - oe.created_at)) * 1000, 1) || '|' ||
  round(extract(epoch FROM (max(dl.created_at) - oe.created_at)) * 1000, 1) || '|' ||
  count(DISTINCT n.id) || '|' || count(DISTINCT dl.id) || '|' ||
  fd.next_chunk_index || '|' || fd.retry_count || '|' || fd.status
FROM post.outbox_events oe
JOIN notification.fanout_dispatches fd ON fd.event_id = (oe.payload->>'eventId')::uuid
LEFT JOIN notification.notifications n ON n.source_event_id = (oe.payload->>'eventId')::uuid
LEFT JOIN notification.notification_delivery_log dl ON dl.notification_id = n.id
WHERE oe.aggregate_id = $postId AND oe.event_type = 'PostPublished'
GROUP BY oe.created_at, fd.updated_at, fd.next_chunk_index, fd.retry_count, fd.status;
"@

$deliveryStatus = Invoke-Psql -TuplesOnly -Sql @"
SELECT status || ':' || count(*)
FROM notification.notification_delivery_log dl
JOIN notification.notifications n ON n.id = dl.notification_id
WHERE n.source_event_id = '$eventId'
GROUP BY status
ORDER BY status;
"@

$timingParts = $timing.Split('|')
$result = [ordered]@{
    runId = $runId
    subscriberCount = $SubscriberCount
    authorId = $authorId
    postId = $postId
    eventId = $eventId
    seedMilliseconds = $seedWatch.ElapsedMilliseconds
    publishHttpMilliseconds = $publishWatch.ElapsedMilliseconds
    dispatcherDoneMilliseconds = [decimal]$timingParts[0]
    notificationsCompleteMilliseconds = [decimal]$timingParts[1]
    deliveryRowsCompleteMilliseconds = [decimal]$timingParts[2]
    notificationCount = [long]$timingParts[3]
    deliveryRowCount = [long]$timingParts[4]
    chunkCount = [int]$timingParts[5]
    retryCount = [int]$timingParts[6]
    dispatchStatus = $timingParts[7]
    throughputPerSecond = [math]::Round($SubscriberCount / ([double]$timingParts[1] / 1000), 1)
    deliveryStatus = ($deliveryStatus -join ",")
    passedFiveSecondSla = ([decimal]$timingParts[2] -le 5000 -and [long]$timingParts[4] -eq $SubscriberCount)
}

$result | ConvertTo-Json
if (-not $complete) { throw "Timed out before all rows were created: $lastProgress" }
