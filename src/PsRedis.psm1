

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
        [Parameter()]
        [string]
        $Connection,

        [switch]
        $NoOutput
    )

    if (!(Test-RedisIsConnected $Global:RedisCacheConnection))
    {
        if (!$NoOutput)
        {
            Write-Host "==> Creating Redis Connection"
        }

        if ([string]::IsNullOrWhiteSpace($Connection))
        {
            throw 'No connection string supplied when creating connection to Redis'
        }

        $Global:RedisServerConnection = $null
        $Global:RedisCacheConnection = [StackExchange.Redis.ConnectionMultiplexer]::Connect($Connection, $null)
        if (!$?)
        {   
            throw 'Failed to create connection to Redis'
        }
    }

    $server = $Global:RedisCacheConnection.GetEndPoints()[0]

    if (!(Test-RedisIsConnected $Global:RedisServerConnection))
    {
        if (!$NoOutput)
        {
            Write-Host "==> Server: $($server -ireplace 'Unspecified/', '')"
        }

        $Global:RedisServerConnection = $Global:RedisCacheConnection.GetServer($server)
        if (!$?)
        {
            throw "Failed to open connection to server"
        }

        if (!$NoOutput)
        {
            Write-Host "==> Version $($Global:RedisServerConnection.Version)"
        }
    }

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
        [Parameter()]
        [string]
        $Connection,

        [switch]
        $Close
    )

    Add-RedisDll

    $conn = Get-RedisConnection -Connection $Connection
    $k = 0

    if (($conn.Info() | Select-Object -Last 1)[0].Value -imatch 'keys=(\d+)')
    {
        $k = $Matches[1]
    }

    Remove-RedisConnection -Close:$Close

    return $k
}

function Get-RedisInfo
{
    param (
        [Parameter()]
        [string]
        $Connection,

        [switch]
        $Close
    )

    Add-RedisDll

    $conn = Get-RedisConnection -Connection $Connection
    $info = $conn.Info()
    Remove-RedisConnection -Close:$Close

    return $info
}

function Get-RedisUptime
{
    param (
        [Parameter()]
        [string]
        $Connection,

        [Parameter()]
        [ValidateSet('Seconds', 'Days')]
        [string]
        $Granularity,

        [switch]
        $Close
    )

    $info = Get-RedisInfo -Connection $Connection -Close:$Close
    $key = "uptime_in_$($Granularity.ToLowerInvariant())"
    return ($info[0] | Where-Object { $_.Key -ieq $key } | Select-Object -ExpandProperty Value)
}

function Set-RedisKey
{
    param (
        [Parameter()]
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
    $value = $db.StringSet($Key, $Value, $TimeOut)
    Remove-RedisConnection -Close:$Close

    return $value
}

function Remove-RedisKeys
{
    param (
        [Parameter()]
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

        Remove-RedisKey -Key $k | Out-Null
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
        [Parameter()]
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

    $end = [DateTime]::UtcNow.Subtract($start)
    Write-Host "`n==> Duration: $($end.ToString())"

    Remove-RedisConnection -Close:$Close

    return $count
}

function Get-RedisKeys
{
    param (
        [Parameter()]
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
        [Parameter()]
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

function Remove-RedisKey
{
    param (
        [Parameter()]
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
    $db.KeyDelete($Key) | Out-Null
    Remove-RedisConnection -Close:$Close
}

function Set-RedisIncrementKey
{
    param (
        [Parameter()]
        [string]
        $Connection,

        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key,

        [Parameter()]
        [int]
        $Increment = 1,

        [switch]
        $Close
    )

    Add-RedisDll

    Get-RedisConnection -Connection $Connection | Out-Null
    $db = Get-RedisDatabase
    $value = $db.StringIncrement($Key, $Increment) | Out-Null
    Remove-RedisConnection -Close:$Close

    return $value
}

function Test-RedisTimings
{
    param (
        [Parameter()]
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
        $NoSleep,

        [switch]
        $Reconnect,

        [switch]
        $Close
    )

    Add-RedisDll

    if ($Seconds -le 0)
    {
        $Seconds = 1
    }

    if (!$Reconnect)
    {
        Get-RedisConnection -Connection $Connection | Out-Null
        $db = Get-RedisDatabase
    }

    Write-Host "`n==> Getting average timing from Redis over $($Seconds)s"

    $startTime = [DateTime]::UtcNow
    $times = @()

    # run and get duration for each call
    while ([DateTime]::UtcNow.Subtract($startTime).TotalSeconds -le $Seconds)
    {
        $_start = [DateTime]::UtcNow

        if ($Reconnect)
        {
            Get-RedisConnection -Connection $Connection -NoOutput | Out-Null
            $db = Get-RedisDatabase
        }

        Set-RedisIncrementKey -Key $Key -Increment 1 | Out-Null

        if ($Reconnect)
        {
            Remove-RedisConnection -Close
        }

        $duration = [DateTime]::UtcNow.Subtract($_start).TotalMilliseconds
        $times += $duration

        if (!$NoSleep -and $duration -lt 1000)
        {
            Start-Sleep -Milliseconds (1000 - $duration)
        }
    }

    # remove the key
    if ($Reconnect)
    {
        Get-RedisConnection -Connection $Connection -NoOutput | Out-Null
        $db = Get-RedisDatabase
    }

    Remove-RedisKey -Key $Key

    # loop through the duration, getting the average/min and max times
    $results = ($times | Measure-Object -Average -Minimum -Maximum)
    Write-Host "==> Average $($results.Average)"
    Write-Host "==> Minimum $($results.Minimum)"
    Write-Host "==> Maximum $($results.Maximum)"

    # close the redis connection
    Remove-RedisConnection -Close:$Close
}