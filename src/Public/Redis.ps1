function Initialize-RedisConnection
{
    [CmdletBinding()]
    param (
        [Parameter()]
        [string]
        $ConnectionString,

        [switch]
        $ReturnConnection
    )

    Add-RedisDll

    if (!(Test-RedisIsConnected $Global:RedisCacheConnection))
    {
        if ([string]::IsNullOrWhiteSpace($ConnectionString)) {
            throw 'No connection string supplied when creating connection to Redis'
        }

        $Global:RedisServerConnection = $null
        $Global:RedisCacheConnection = [StackExchange.Redis.ConnectionMultiplexer]::Connect($ConnectionString, $null)
        if (!$?) {
            throw 'Failed to create connection to Redis'
        }
    }

    $server = $Global:RedisCacheConnection.GetEndPoints()[0]

    if (!(Test-RedisIsConnected $Global:RedisServerConnection))
    {
        $Global:RedisServerConnection = $Global:RedisCacheConnection.GetServer($server)
        if (!$?) {
            throw "Failed to open connection to server"
        }
    }

    if ($ReturnConnection) {
        return $Global:RedisServerConnection
    }
}

function Close-RedisConnection
{
    [CmdletBinding()]
    param()

    if (Test-RedisIsConnected $Global:RedisCacheConnection)
    {
        $Global:RedisCacheConnection.Dispose()
        if (!$?) {
            throw "Failed to dispose Redis connection"
        }

        $Global:RedisCacheConnection = $null
    }
}

function Get-RedisDatabase
{
    [CmdletBinding()]
    param()

    if (!(Test-RedisIsConnected $Global:RedisCacheConnection)) {
        throw "No Redis connection has been initialized"
    }

    return $Global:RedisCacheConnection.GetDatabase($Global:DatabaseIndex)
}

function Get-RedisConnection
{
    [CmdletBinding()]
    param()

    if ($null -eq $Global:RedisServerConnection) {
        throw "No Redis connection has been initialized"
    }

    return $Global:RedisServerConnection
}

function Get-RedisInfoKeys
{
    [CmdletBinding()]
    param()

    $conn = Get-RedisConnection
    $k = 0

    if (($conn.Info() | Select-Object -Last 1)[0].Value -imatch 'keys=(\d+)') {
        $k = $Matches[1]
    }

    return $k
}

function Get-RedisInfo
{
    [CmdletBinding()]
    param()

    $conn = Get-RedisConnection
    $info = $conn.Info()

    return $info
}

function Get-RedisUptime
{
    [CmdletBinding()]
    param (
        [Parameter()]
        [ValidateSet('Seconds', 'Days')]
        [string]
        $Granularity
    )

    $info = Get-RedisInfo
    $key = "uptime_in_$($Granularity.ToLowerInvariant())"
    return ($info[0] | Where-Object { $_.Key -ieq $key } | Select-Object -ExpandProperty Value)
}

function Set-RedisKey
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key,

        [Parameter()]
        [string]
        $Value,

        [Parameter()]
        [timespan]
        $TimeOut
    )

    $db = Get-RedisDatabase
    $value = $db.StringSet($Key, $Value, $TimeOut)

    return $value
}

function Remove-RedisKeys
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Pattern
    )

    $conn = Get-RedisConnection
    $count = 0

    foreach ($k in $conn.Keys($Global:DatabaseIndex, $Pattern))
    {
        Remove-RedisKey -Key $k | Out-Null
        if (!$?) {
            throw "Failed to delete key: $($k)"
        }

        $count++
    }

    return $count
}

function Get-RedisKeysCount
{
    [CmdletBinding()]
    param (
        [Parameter()]
        [string]
        $Pattern = '*'
    )

    $conn = Get-RedisConnection

    if ([string]::IsNullOrWhiteSpace($Pattern)) {
        $Pattern = '*'
    }

    $keys = @{}

    foreach ($k in $conn.Keys($Global:DatabaseIndex, $Pattern)) {
        $keys[($k -isplit ':')[0]]++
    }

    return $keys
}

function Get-RedisKeyDetails
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key,

        [Parameter()]
        [string]
        $Type
    )

    if ([string]::IsNullOrWhitespace($Type)){
        $Type = Get-RedisKeyType -Key $Key
    }

    $value = Get-RedisKey -Key $Key -Type $Type

    return @{
        Key = $Key
        Type = $Type
        Value = $value
        TTL = (Get-RedisKeyTTL -Key $Key).TotalSeconds
        Size = (Get-RedisKeyValueLengthPrivate -Data $value)
    }
}

function Get-RedisKey
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key,

        [Parameter()]
        [string]
        $Type
    )

    $db = Get-RedisDatabase

    if ([string]::IsNullOrWhitespace($Type)){
        $Type = Get-RedisKeyType -Key $Key
    }

    switch ($Type.ToLowerInvariant()) {
        'hash' {
            $value = [string]($db.HashGetAll($Key)).Value
        }

        'set' {
            $value = @([string]($db.SetMembers($Key)) -isplit '\s+')
        }

        default {
            $value = ($db.StringGet($Key)).ToString()
        }
    }

    return $value
}

function Get-RedisKeyValueLength
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key
    )

    return Get-RedisKeyValueLengthPrivate -Key $Key
}

function Get-RedisRandomKey
{
    [CmdletBinding()]
    param()

    $db = Get-RedisDatabase
    $value = $db.KeyRandom()

    return $value
}

function Get-RedisKeyType
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key
    )

    $db = Get-RedisDatabase
    $value = $db.KeyType($key)
    return $value.ToString()
}

function Get-RedisKeyTTL
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key
    )

    $db = Get-RedisDatabase
    $value = $db.KeyTimeToLive($Key)

    return $value
}

function Set-RedisKeyTTL
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key,

        [Parameter(Mandatory=$true)]
        [ValidateNotNull()]
        [int]
        $TtlInSeconds
    )

    $db = Get-RedisDatabase
    $db.KeyExpire($Key, [TimeSpan]::FromSeconds($TtlInSeconds)) | Out-Null
}

function Get-RedisKeys
{
    [CmdletBinding()]
    param (
        [Parameter()]
        [string]
        $Pattern = '*',

        [Parameter()]
        [scriptblock]
        $ScriptBlock,

        [Parameter()]
        [int]
        $KeyCount = 0
    )

    $conn = Get-RedisConnection
    $keys = @()

    if ([string]::IsNullOrWhiteSpace($Pattern)) {
        $Pattern = '*'
    }

    foreach ($k in $conn.Keys($Global:DatabaseIndex, $Pattern)) {
        $result = $true
        if ($null -ne $ScriptBlock) {
            $result = (Invoke-Command -ScriptBlock $ScriptBlock -ArgumentList $k)
        }

        if ($result) {
            $keys += $k
        }

        if (($KeyCount -gt 0) -and ($keys.Length -ge $KeyCount)) {
            break
        }
    }

    return $keys
}

function Remove-RedisKey
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key
    )

    $db = Get-RedisDatabase
    $db.KeyDelete($Key) | Out-Null
}

function Remove-RedisSetMembers
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key,

        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string[]]
        $Members
    )

    $db = Get-RedisDatabase
    $db.SetRemove($Key, $Members) | Out-Null
}

function Add-RedisSetMembers
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key,

        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string[]]
        $Members
    )

    $db = Get-RedisDatabase
    $db.SetAdd($Key, $Members) | Out-Null
}

function Set-RedisIncrementKey
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key,

        [Parameter()]
        [int]
        $Increment = 1
    )

    $db = Get-RedisDatabase
    $value = $db.StringIncrement($Key, $Increment) | Out-Null

    return $value
}

function Test-RedisTimings
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key,

        [Parameter()]
        [int]
        $Seconds = 120,

        [switch]
        $NoSleep
    )

    if ($Seconds -le 0) {
        $Seconds = 1
    }

    $startTime = [DateTime]::UtcNow
    $times = @()

    # run and get duration for each call
    while ([DateTime]::UtcNow.Subtract($startTime).TotalSeconds -le $Seconds)
    {
        $_start = [DateTime]::UtcNow

        Set-RedisIncrementKey -Key $Key -Increment 1 | Out-Null

        $duration = [DateTime]::UtcNow.Subtract($_start).TotalMilliseconds
        $times += $duration

        if (!$NoSleep -and $duration -lt 1000) {
            Start-Sleep -Milliseconds (1000 - $duration)
        }
    }

    # remove the key
    Remove-RedisKey -Key $Key

    # loop through the duration, getting the average/min and max times
    $results = ($times | Measure-Object -Average -Minimum -Maximum)

    return @{
        Average = $results.Average
        Minimum = $results.Minimum
        Maximum = $results.Maximum
    }
}