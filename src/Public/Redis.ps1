<#
.SYNOPSIS
Initializes the connection with the redis server

.DESCRIPTION
Initializes the connection with the redis server

.Parameter ConnectionString
The connection string to connect to the redis server. Example: "redisUrl.com:6380,password=PaSSwOrd,ssl=True,abortConnect=False"

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

        [Parameter()]
        $ConnectionName
    )

    # first, disconnect any existing connection
    Disconnect-Redis -ConnectionName $ConnectionName

    if ($null -eq $ConnectionName)
    {
        $ConnectionName = "__default__"
    }

    # open a new connection
    if (!(Test-RedisIsConnected $Global:PsRedisCacheConnections[$ConnectionName]))
    {
        if ([string]::IsNullOrWhiteSpace($ConnectionString))
        {
            throw 'No connection string supplied when creating connection to Redis'
        }

        $Global:PsRedisServerConnections[$ConnectionName] = $null
        $Global:PsRedisCacheConnections[$ConnectionName] = [StackExchange.Redis.ConnectionMultiplexer]::Connect($ConnectionString, $null)
        if (!$?)
        {
            throw 'Failed to create connection to Redis'
        }
    }

    # set the redis server
    $server = $Global:PsRedisCacheConnections[$ConnectionName].GetEndPoints()[0]

    if (!(Test-RedisIsConnected $Global:PsRedisServerConnections[$ConnectionName]))
    {
        $Global:PsRedisServerConnections[$ConnectionName] = $Global:PsRedisCacheConnections[$ConnectionName].GetServer($server)
        if (!$?)
        {
            throw "Failed to open connection to server"
        }
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
    param(
        [Parameter()]
        $ConnectionName
    )

    if ([string]::IsNullOrWhiteSpace($ConnectionName))
    {
        $ConnectionName = "__default__"
    }

    $connection = $Global:PsRedisCacheConnections[$ConnectionName]

    if (Test-RedisIsConnected $connection)
    {
        $connection.Dispose()
        if (!$?) {
            throw "Failed to dispose Redis connection"
        }

        $connection = $null
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
        $Arguments,

        [Parameter()]
        $ConnectionName
    )

    # connect to redis
    Connect-Redis -ConnectionString $ConnectionString -ConnectionName $ConnectionName

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
        Disconnect-Redis -ConnectionName $ConnectionName
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
    param(
        [Parameter()]
        $ConnectionName
    )

    $conn = Get-RedisConnection -ConnectionName $ConnectionName
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
    param(
        [Parameter()]
        $ConnectionName
    )

    $conn = Get-RedisConnection -ConnectionName $ConnectionName
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
        $Granularity,

        [Parameter()]
        $ConnectionName
    )

    $info = Get-RedisInfo -ConnectionName $ConnectionName
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

.Parameter TTL
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
        $TTL,

        [Parameter()]
        $ConnectionName,

        [switch]
        $HashType
    )

    $db = Get-RedisDatabase -ConnectionName $ConnectionName

    if ($HashType){
        $value = $db.HashSet($Key, $Value, 0)

        Set-RedisKeyTTL -Key $Key -TTL $TTL.TotalSeconds -ConnectionName $ConnectionName
    }
    else{
        $value = $db.StringSet($Key, $Value, $TTL)
    }


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
        $Pattern,

        [Parameter()]
        $ConnectionName
    )

    $conn = Get-RedisConnection -ConnectionName $ConnectionName
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
        $Pattern = '*',

        [Parameter()]
        $ConnectionName
    )

    $conn = Get-RedisConnection -ConnectionName $ConnectionName

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
        $Type,

        [Parameter()]
        $ConnectionName
    )

    if ([string]::IsNullOrWhitespace($Type)){
        $Type = Get-RedisKeyType -Key $Key -ConnectionName $ConnectionName
    }

    $value = Get-RedisKey -Key $Key -Type $Type -ConnectionName $ConnectionName

    return @{
        Key = $Key
        Type = $Type
        Value = $value
        TTL = (Get-RedisKeyTTL -Key $Key -ConnectionName $ConnectionName).TotalSeconds
        Size = (Get-RedisKeyValueLengthPrivate -Data $value -ConnectionName $ConnectionName)
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
        $Type,

        [Parameter()]
        $ConnectionName,

        [switch]
        $HashAsHashEntry
    )

    $db = Get-RedisDatabase -ConnectionName $ConnectionName

    if ([string]::IsNullOrWhitespace($Type)){
        $Type = Get-RedisKeyType -Key $Key -ConnectionName $ConnectionName
    }

    switch ($Type.ToLowerInvariant()) {
        'hash' {
            if ($HashAsHashEntry)
            {
                $value = $db.HashGetAll($Key)
            }
            else{
                $value = [string]($db.HashGetAll($Key)).Value
            }
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
        $Key,

        [Parameter()]
        $ConnectionName
    )

    return Get-RedisKeyValueLengthPrivate -Key $Key -ConnectionName $ConnectionName
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
    param(
        [Parameter()]
        $ConnectionName
    )

    $db = Get-RedisDatabase -ConnectionName $ConnectionName
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
        $KeyCount = 1,

        [Parameter()]
        $ConnectionName
    )

    $keys = @()

    if ($KeyCount -le 1){
        $KeyCount = 1
    }

    while ($keys.Length -lt $KeyCount){
        $key = [string](Get-RedisRandomKey -ConnectionName $ConnectionName)

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
        $PageSize = 10,

        [Parameter()]
        $ConnectionName
    )

    $keys = @()
    $progress = 0

    if ($KeyCount -le 1) {
        $KeyCount = 1
    }

    while ($keys.Length -lt $KeyCount) {
        $keys += (Get-RedisKeys -Pattern $Pattern -KeyCount ($KeyCount - $keys.Length) -KeyOffset $KeyOffset -PageSize $PageSize -ConnectionName $ConnectionName -ScriptBlock {
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

<#
.SYNOPSIS
Get a key's type from Redis.

.DESCRIPTION
Get a key's type from Redis.

.PARAMETER Key
The Key name to lookup.

.EXAMPLE
Get-RedisKeyType -Key 'UserId:123'
#>
function Get-RedisKeyType
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key,

        [Parameter()]
        $ConnectionName
    )

    $db = Get-RedisDatabase -ConnectionName $ConnectionName
    $value = $db.KeyType($key)
    return $value.ToString()
}

<#
.SYNOPSIS
Get a key's TTL from Redis.

.DESCRIPTION
Get a key's TTL from Redis.

.PARAMETER Key
The Key name to lookup.

.EXAMPLE
Get-RedisKeyTTL -Key 'UserId:123'
#>
function Get-RedisKeyTTL
{
    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$true)]
        [ValidateNotNullOrEmpty()]
        [string]
        $Key,

        [Parameter()]
        $ConnectionName
    )

    $db = Get-RedisDatabase -ConnectionName $ConnectionName
    $value = $db.KeyTimeToLive($Key)
    return $value
}

<#
.SYNOPSIS
Set a key's TTL in Redis.

.DESCRIPTION
Set a key's TTL in Redis.

.PARAMETER Key
The Key to update.

.PARAMETER TTL
The TTL, in seconds.

.EXAMPLE
Set-RedisKeyTTL -Key 'UserId:123' -TTL 3600
#>
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
        $TTL,

        [Parameter()]
        $ConnectionName
    )

    $db = Get-RedisDatabase -ConnectionName $ConnectionName
    $db.KeyExpire($Key, [TimeSpan]::FromSeconds($TTL)) | Out-Null
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
        $PageSize = 10,

        [Parameter()]
        $ConnectionName
    )

    $conn = Get-RedisConnection -ConnectionName $ConnectionName
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
        $Key,

        [Parameter()]
        $ConnectionName
    )

    $db = Get-RedisDatabase -ConnectionName $ConnectionName
    $db.KeyDelete($Key) | Out-Null
}

function Remove-RedisSetMember
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
        $Member,

        [Parameter()]
        $ConnectionName
    )

    $db = Get-RedisDatabase -ConnectionName $ConnectionName
    $db.SetRemove($Key, $Member) | Out-Null
}

function Add-RedisSetMember
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
        $Member,

        [Parameter()]
        $ConnectionName
    )

    $db = Get-RedisDatabase -ConnectionName $ConnectionName
    $db.SetAdd($Key, $Member) | Out-Null
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
        $Increment = 1,

        [Parameter()]
        $ConnectionName
    )

    $db = Get-RedisDatabase -ConnectionName $ConnectionName
    $value = $db.StringIncrement($Key, $Increment) | Out-Null
    return $value
}

function Test-RedisTiming
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
        $NoSleep,

        [Parameter()]
        $ConnectionName
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

        Set-RedisIncrementKey -Key $Key -Increment 1 -ConnectionName $ConnectionName | Out-Null

        $duration = [DateTime]::UtcNow.Subtract($_start).TotalMilliseconds
        $times += $duration

        if (!$NoSleep -and $duration -lt 1000) {
            Start-Sleep -Milliseconds (1000 - $duration)
        }
    }

    # remove the key
    Remove-RedisKey -Key $Key -ConnectionName $ConnectionName

    # loop through the duration, getting the average/min and max times
    $results = ($times | Measure-Object -Average -Minimum -Maximum)

    return @{
        Average = $results.Average
        Minimum = $results.Minimum
        Maximum = $results.Maximum
    }
}