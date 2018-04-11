

$Global:RedisCacheConnection = $null
$Global:RedisServerConnection = $null
$Global:DatabaseIndex = 0

function Add-RedisDll
{
    Add-Type -Path '.\packages\StackExchange.Redis.1.2.6\lib\net45\StackExchange.Redis.dll' -ErrorAction Stop | Out-Null
}

function Test-RedisIsConnected
{
    param (
        [Parameter()]
        $Connection
    )

    return ($Connection -ne $null -and $Connection.IsConnected)
}

function Get-RedisConnection
{
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Connection
    )

    if (!(Test-RedisIsConnected $Global:RedisCacheConnection))
    {
        Write-Host "==> Creating Redis Connection"

        $Global:RedisServerConnection = $null
        $Global:RedisCacheConnection = [StackExchange.Redis.ConnectionMultiplexer]::Connect($Connection, $null)
        if (!$?)
        {   
            throw "Failed to create connection to Redis"
        }
    }

    $server = $Global:RedisCacheConnection.GetEndPoints()[0]
    Write-Host "==> Server: $($server -ireplace 'Unspecified/', '')"

    if (!(Test-RedisIsConnected $Global:RedisServerConnection))
    {
        $Global:RedisServerConnection = $Global:RedisCacheConnection.GetServer($server)
        if (!$?)
        {
            throw "Failed to open connection to server"
        }
    }

    Write-Host "==> Version $($Global:RedisServerConnection.Version)"
    return $Global:RedisServerConnection
}

function Get-RedisDatabase
{
    if (!(Test-RedisIsConnected $Global:RedisCacheConnection))
    {
        throw "No Redis connection has been established"
    }

    return $Global:RedisCacheConnection.GetDatabase($Global:DatabaseIndex)
}

function Remove-RedisConnection
{
    param (
        [switch]
        $Close
    )

    if (!$Close)
    {
        return
    }

    if (Test-RedisIsConnected $Global:RedisCacheConnection)
    {
        $Global:RedisCacheConnection.Dispose()
        if (!$?)
        {
            throw "Failed to dispose Redis connection"
        }

        $Global:RedisCacheConnection = $null
    }
}

function Get-RedisInfoKeys
{
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Connection,

        [switch]
        $Close
    )

    Add-RedisDll

    $conn = Get-RedisConnection -Connection $Connection

    if (!(Test-RedisIsConnected $conn))
    {
        throw "No Redis server connection has been established"
    }

    $k = 0

    if (($conn.Info() | Select-Object -Last 1)[0].Value -imatch 'keys=(\d+)')
    {
        $k = $Matches[1]
    }

    Write-Host "==> Keys: $($k)"
    Remove-RedisConnection -Close:$Close

    return $k
}

function Get-RedisInfo
{
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Connection,

        [switch]
        $Close
    )

    Add-RedisDll

    $conn = Get-RedisConnection -Connection $Connection
    Remove-RedisConnection -Close:$Close

    return $conn.Info()
}

function Set-RedisKey
{
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Connection,

        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key,

        [Parameter()]
        [string]
        $Value,

        [Parameter()]
        [timespan]
        $TimeOut,

        [switch]
        $Close
    )

    Add-RedisDll

    Get-RedisConnection -Connection $Connection | Out-Null
    $db = Get-RedisDatabase

    Write-Host "`n==> Setting Key: $($Key) - $($Value) [ttl: $($TimeOut)]"
    $start = [DateTime]::UtcNow

    $db.StringSet($Key, $Value, $TimeOut) | Out-Null

    Write-Host "`n==> Key inserted"

    $end = [DateTime]::UtcNow.Subtract($start)
    Write-Host "`n==> Duration: $($end.ToString())"

    Remove-RedisConnection -Close:$Close
}

function Remove-RedisKeys
{
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Connection,

        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Pattern,

        [int]
        $SleepThreshold = 0,

        [int]
        $SleepSeconds = 10,

        [int]
        $MaxDelete = 0,

        [switch]
        $Close
    )

    Add-RedisDll

    $conn = Get-RedisConnection -Connection $Connection
    $db = Get-RedisDatabase

    Write-Host "`n==> Deleting Keys: $($Pattern)"
    Get-RedisInfoKeys | Out-Null

    $count = 0
    $start = [DateTime]::UtcNow

    foreach ($k in $conn.Keys($Global:DatabaseIndex, $Pattern))
    {
        if ($count -gt 0 -and $SleepThreshold -gt 0 -and $count % $SleepThreshold -eq 0)
        {
            Write-Host "==> Deleted: $($count) [Sleep: $($SleepSeconds)s]"
            Start-Sleep -Seconds $SleepSeconds
        }

        $db.KeyDelete($k) | Out-Null
        if (!$?)
        {
            throw "Failed to delete key: $($k)"
        }

        $count++

        if ($MaxDelete -gt 0 -and $count -ge $MaxDelete)
        {
            Write-Host "==> Deleted: $($count)"
            break
        }
    }

    Write-Host "`n==> Total Keys Deleted: $($count)"
    Get-RedisInfoKeys | Out-Null

    $end = [DateTime]::UtcNow.Subtract($start)
    Write-Host "`n==> Duration: $($end.ToString())"

    Remove-RedisConnection -Close:$Close
}

function Get-RedisKeysCount
{
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Connection,

        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Pattern,

        [switch]
        $Close
    )

    Add-RedisDll

    $conn = Get-RedisConnection -Connection $Connection

    Write-Host "`n==> Counting Keys: $($Pattern)"

    $count = 0
    $start = [DateTime]::UtcNow

    foreach ($k in $conn.Keys($Global:DatabaseIndex, $Pattern))
    {
        $count++
    }

    Write-Host "`n==> Count: $($count)"

    $end = [DateTime]::UtcNow.Subtract($start)
    Write-Host "`n==> Duration: $($end.ToString())"

    Remove-RedisConnection -Close:$Close

    return $count
}

function Get-RedisKeys
{
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Connection,

        [switch]
        $Close
    )

    Add-RedisDll

    $conn = Get-RedisConnection -Connection $Connection

    Write-Host "`n==> Retrieving all Keys"

    $keys = @()
    $start = [DateTime]::UtcNow

    foreach ($k in $conn.Keys($Global:DatabaseIndex, '*'))
    {
        $keys += $k
    }

    $end = [DateTime]::UtcNow.Subtract($start)
    Write-Host "`n==> Duration: $($end.ToString())"

    Remove-RedisConnection -Close:$Close

    return $keys
}

function Get-RedisKey
{
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Connection,

        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key,

        [switch]
        $Close
    )

    Add-RedisDll

    Get-RedisConnection -Connection $Connection | Out-Null
    $db = Get-RedisDatabase
    $value = $db.StringGet($Key)
    Remove-RedisConnection -Close:$Close

    return $value
}

function Invoke-RedisTiming
{
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Connection,

        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key,

        [Parameter()]
        [int]
        $Seconds = 120,

        [switch]
        $Close
    )

    Add-RedisDll

    if ($Seconds -le 0)
    {
        $Seconds = 1
    }

    Get-RedisConnection -Connection $Connection | Out-Null
    $db = Get-RedisDatabase

    Write-Host "`n==> Getting average timing from Redis over $($Seconds)s"

    $count = 0
    $times = @()

    # run for ~2mins, and get duration for each call
    while ($count -lt $Seconds)
    {
        $count++
        $start = [DateTime]::UtcNow

        $db.StringIncrement($Key, 1) | Out-Null

        $duration = [DateTime]::UtcNow.Subtract($start).TotalMilliseconds
        $times += $duration

        if ($duration -lt 1000)
        {
            Start-Sleep -Milliseconds (1000 - $duration)
        }
    }

    # remove the key
    $db.KeyDelete($Key) | Out-Null

    # loop through the duration, getting the average/min and max times
    $results = ($times | Measure-Object -Average -Minimum -Maximum)
    Write-Host "==> Average $($results.Average)"
    Write-Host "==> Minimum $($results.Minimum)"
    Write-Host "==> Maximum $($results.Maximum)"

    # close the redis connection
    Remove-RedisConnection -Close:$Close
}