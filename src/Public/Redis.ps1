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

    if (!(Test-RedisIsConnected $Global:PsRedisCacheConnection))
    {
        if ([string]::IsNullOrWhiteSpace($ConnectionString)) {
            throw 'No connection string supplied when creating connection to Redis'
        }

        $Global:PsRedisServerConnection = $null
        $Global:PsRedisCacheConnection = [StackExchange.Redis.ConnectionMultiplexer]::Connect($ConnectionString, $null)
        if (!$?) {
            throw 'Failed to create connection to Redis'
        }
    }

    $server = $Global:PsRedisCacheConnection.GetEndPoints()[0]

    if (!(Test-RedisIsConnected $Global:PsRedisServerConnection))
    {
        $Global:PsRedisServerConnection = $Global:PsRedisCacheConnection.GetServer($server)
        if (!$?) {
            throw "Failed to open connection to server"
        }
    }

    if ($ReturnConnection) {
        return $Global:PsRedisServerConnection
    }
}

function Close-RedisConnection
{
    [CmdletBinding()]
    param()

    if (Test-RedisIsConnected $Global:PsRedisCacheConnection)
    {
        $Global:PsRedisCacheConnection.Dispose()
        if (!$?) {
            throw "Failed to dispose Redis connection"
        }

        $Global:PsRedisCacheConnection = $null
    }
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

    foreach ($k in $conn.Keys($Global:PsRedisDatabaseIndex, $Pattern))
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

    foreach ($k in $conn.Keys($Global:PsRedisDatabaseIndex, $Pattern)) {
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

function Get-RedisRandomKeys
{
    [CmdletBinding()]
    param (
        [Parameter()]
        [string]
        $Pattern = '',

        [Parameter()]
        [scriptblock]
        $ScriptBlock,

        [Parameter(Mandatory=$true)]
        [int]
        $KeyCount = 1
    )

    $keys = @()

    if ($KeyCount -le 1){
        $KeyCount = 1
    }

    while ($keys.Length -lt $KeyCount){
        $key = [string](Get-RedisRandomKey)

        if (!([string]::IsNullOrWhiteSpace($Pattern)) -and !($key -imatch $Pattern)) {
            continue
        }

        $result = $true
        if ($null -ne $ScriptBlock) {
            $result = (Invoke-Command -ScriptBlock $ScriptBlock -ArgumentList $key)
        }

        if ($result) {
            $keys += $k
        }

        $i = ($keys.Length / $KeyCount) * 100
        Write-Progress -Activity "Search in Progress" -Status "$i% Complete:" -PercentComplete $i
    }

    return $keys
}

function Get-RedisRandomKeysQuick
{
    [CmdletBinding()]
    param (
        [Parameter()]
        [string]
        $Pattern = '*',

        [Parameter()]
        [scriptblock]
        $ScriptBlock,

        [Parameter(Mandatory=$true)]
        [int]
        $KeyCount = 1,

        [Parameter()]
        [int]
        $KeyOffset = 0,

        [Parameter()]
        [int]
        $PageSize = 10
    )

    $keys = @()
    $progress = 0

    if ($KeyCount -le 1) {
        $KeyCount = 1
    }

    while ($keys.Length -lt $KeyCount) {
        $keys += (Get-RedisKeys -Pattern $Pattern -KeyCount ($KeyCount - $keys.Length) -KeyOffset $KeyOffset -PageSize $PageSize -ScriptBlock {
            param($key)
            $allowed = ((Get-Random -Minimum 1 -Maximum 5) -eq 2)

            if ($allowed) {
                if ($null -ne $script:ScriptBlock) {
                    $allowed = (Invoke-Command -ScriptBlock $script:ScriptBlock -ArgumentList $key)
                }

                if ($allowed) {
                    $script:progress++
                    $i = ($script:progress / $script:KeyCount) * 100
                    Write-Progress -Activity "Search in Progress" -Status "$i% Complete:" -PercentComplete $i
                }
            }

            return $allowed
        })
    }

    return $keys
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
        $KeyCount = 0,

        [Parameter()]
        [int]
        $KeyOffset = 0,

        [Parameter()]
        [int]
        $PageSize = 10
    )

    $conn = Get-RedisConnection
    $keys = @()

    if ([string]::IsNullOrWhiteSpace($Pattern)) {
        $Pattern = '*'
    }

    if ($KeyOffset -lt 0) {
        $KeyOffset = 0
    }

    if ($PageSize -lt 0) {
        $PageSize = 10
    }

    foreach ($k in $conn.Keys($Global:PsRedisDatabaseIndex, $Pattern, $PageSize, 0, $KeyOffset)) {
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