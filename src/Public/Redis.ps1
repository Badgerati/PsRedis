<#
.SYNOPSIS
Initializes the connection with the redis server

.DESCRIPTION
Initializes the connection with the redis server

.Parameter ConnectionString
The connection string to connect to the redis server. Example: "redisUrl.com:6380,password=PaSSwOrd,ssl=True,abortConnect=False"

.Parameter ReturnConnection
Switch for if the connection should be returned.

.EXAMPLE
Connect-Redis -ConnectionString "redisUrl.com:6380,password=PaSSwOrd,ssl=True,abortConnect=False"

#>
function Connect-Redis
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [string]
        $ConnectionString,

        [switch]
        $ReturnConnection
    )

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

<#
.SYNOPSIS
Closes the connection with the redis server

.DESCRIPTION
Closes the connection with the redis server

.EXAMPLE
Disconnect-Redis

#>
function Disconnect-Redis
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

<#
.SYNOPSIS
Connects to Redis, invokes a script, and then disconencts the session.

.DESCRIPTION
Connects to Redis, invokes a script, and then disconencts the session.

.PARAMETER ConnectionString
The connection string to connect to the redis server. Example: "redisUrl.com:6380,password=PaSSwOrd,ssl=True,abortConnect=False"

.PARAMETER ScriptBlock
The ScriptBlock to be invoked with other PsRedis functions.

.PARAMETER Arguments
Any options Arguments to supply tot eh ScriptBlock

.EXAMPLE
Invoke-RedisScript -ConnectionString 'redisUrl.com:6380' -ScriptBlock { Get-RedisInfo }
#>

function Invoke-RedisScript
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [string]
        $ConnectionString,

        [Parameter(Mandatory=$true)]
        [ValidateNotNull()]
        [scriptblock]
        $ScriptBlock,

        [Parameter()]
        [object[]]
        $Arguments
    )

    # connect to redis
    Connect-Redis -ConnectionString $ConnectionString

    try {
        # run the script
        if (($null -eq $Arguments) -or ($Arguments.Length -eq 0)) {
            . $ScriptBlock
        }
        else {
            . $ScriptBlock @Arguments
        }
    }
    finally {
        # disconnect from redis
        Disconnect-Redis
    }
}

<#
.SYNOPSIS
Gets the keys section of the redis info command

.DESCRIPTION
Gets the keys section of the redis info command

.EXAMPLE
Get-RedisInfoKeys

#>
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

<#
.SYNOPSIS
Gets the results of the redis info command

.DESCRIPTION
Gets the results of the redis info command

.EXAMPLE
Get-RedisInfo

#>
function Get-RedisInfo
{
    [CmdletBinding()]
    param()

    $conn = Get-RedisConnection
    $info = $conn.Info()

    return $info
}

<#
.SYNOPSIS
Gets the uptime of the redis server

.DESCRIPTION
Gets the uptime of the redis server

.Parameter Granularity
Sets the granularity of the up time of the redis server. Can be either Seconds or Days

.EXAMPLE
Get-RedisUptime -Granularity 'Seconds'

#>
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

<#
.SYNOPSIS
Adds a new string redis key

.DESCRIPTION
Adds a new string redis key

.Parameter Key
The name of the key being added

.Parameter Value
The value of the key being added

.Parameter TimeOut
(Optional) When the key will expire. If not passed then a expire time will not be set

.EXAMPLE
Add-RedisKey -Key 'SessionGuid' -Value 'SessionData'

#>
function Add-RedisKey
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

<#
.SYNOPSIS
Removes all keys with a supplied pattern

.DESCRIPTION
Removes all keys with a supplied pattern

.Parameter Pattern
The pattern to match the keys to be removed. Example '*' will remove all keys, 'Session*' will remove all keys that start with 'Session'

.EXAMPLE
Remove-RedisKeys -Pattern 'Cheese*'

#>
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

<#
.SYNOPSIS
Gets the count of all the keys with a supplied pattern

.DESCRIPTION
Gets the count of all the keys with a supplied pattern

.Parameter Pattern
The pattern to match the keys to be retrieved. Example '*' will retrieve all keys, 'Session*' will retrieve all keys that start with 'Session'

.EXAMPLE
Get-RedisKeysCount -Pattern 'Cheese*'

#>
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

<#
.SYNOPSIS
Gets the details of a redis key with the supplied key

.DESCRIPTION
Gets the details of a redis key with the supplied key

.Parameter Key
The key name of the key that will be retrieve

.Parameter Type
(Optional) The key type, helps to reduce the amount of round trips if already known

.EXAMPLE
Get-RedisKeyDetails -Key 'Grapes' -Type 'Set'

#>
function Get-RedisKeyDetails
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key,

        [Parameter()]
        [ValidateSet('hash', 'set', 'string')]
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

<#
.SYNOPSIS
Gets the value of a redis key with the supplied key

.DESCRIPTION
Gets the value of a redis key with the supplied key

.Parameter Key
The key name of the key that will be retrieve

.Parameter Type
(Optional) The key type, helps to reduce the amount of round trips if already known

.EXAMPLE
Get-RedisKey -Key 'Grapes' -Type 'Set'

#>
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

<#
.SYNOPSIS
Gets the length of a redis key value. 
If the key is of type set then it will return the amount of items in the set.
Otherwise, it will return the amount of characters in the value

.DESCRIPTION
Gets the length of a redis key value. 
If the key is of type set then it will return the amount of items in the set.
Otherwise, it will return the amount of characters in the value

.Parameter Key
The key name of the key that will be retrieve for the length

.EXAMPLE
Get-RedisKeyValueLength -Key 'Grapes'

#>
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

<#
.SYNOPSIS
Gets a random key from the redis server

.DESCRIPTION
Gets a random key from the redis server

.EXAMPLE
Get-RedisRandomKey

#>
function Get-RedisRandomKey
{
    [CmdletBinding()]
    param()

    $db = Get-RedisDatabase
    $value = $db.KeyRandom()

    return $value
}

<#
.SYNOPSIS
Gets random keys from the redis server  matching a supplied pattern

.DESCRIPTION
Gets random keys from the redis server  matching a supplied pattern

.Parameter Pattern
The pattern then the key name needs to match

.Parameter ScriptBlock
A script block that will be ran for each key that matches the pattern.
Return a value of $false to make the key not count to the total

.Parameter KeyCount
The amount of keys to retrieve

.EXAMPLE
Get-RedisRandomKeys -Pattern 'Toaster*' -ScriptBlock {return $true} -KeyCount 10

#>
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