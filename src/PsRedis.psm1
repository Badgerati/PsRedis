

$Global:RedisCacheConnection = $null
$Global:RedisServerConnection = $null
$Global:DatabaseIndex = 0

function Add-RedisDll
{
    Add-Type -Path '.\packages\StackExchange.Redis.1.2.6\lib\net45\StackExchange.Redis.dll' -ErrorAction Stop | Out-Null
}

function Get-RedisConnection
{
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Connection
    )

    if ($Global:RedisCacheConnection -eq $null -or !$Global:RedisCacheConnection.IsConnected)
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

    if ($Global:RedisServerConnection -eq $null)
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
    if ($Global:RedisCacheConnection -eq $null)
    {
        throw "No Redis connection has been established"
    }

    return $Global:RedisCacheConnection.GetDatabase($Global:DatabaseIndex)
}

function Remove-RedisConnection
{
    if ($Global:RedisCacheConnection -ne $null)
    {
        $Global:RedisCacheConnection.Dispose()
        if (!$?)
        {
            throw "Failed to dispose Redis connection"
        }
    }
}

function Get-RedisInfoKeys
{
    if ($Global:RedisServerConnection -eq $null)
    {
        throw "No Redis server connection has been established"
    }

    $k = 0

    if (($Global:RedisServerConnection.Info() | Select-Object -Last 1)[0].Value -imatch 'keys=(\d+)')
    {
        $k = $Matches[1]
    }

    Write-Host "==> Keys: $($k)"
    return $k
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
        $MaxDelete = 0
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

    Remove-RedisConnection
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
        $Pattern
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

    Remove-RedisConnection
}