# PsRedis

[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/Badgerati/PsRedis/master/LICENSE.txt)
[![PowerShell](https://img.shields.io/powershellgallery/dt/psredis.svg?label=PowerShell&colorB=085298)](https://www.powershellgallery.com/packages/PsRedis)

PowerShell module for interacting with Redis caches, via the StackExchange.Redis library.

> Requires PowerShell 5.0+

## Usage

Before running any of the examples, ensure you have imported the PsRedis module:

```powershell
Import-Module PsRedis
```

Your scripts must first open a connection, and then close it:

```powershell
Connect-Redis -ConnectionString $ConnectionString

# logic

Disconnect-Redis
```

or you can use the helper function that wraps the two:

```powershell
Invoke-RedisScript -ConnectionString $ConnectionString -ScriptBlock {
    # logic
}
```

### Return INFO about a Cache

This example will return the information you get from running the `INFO` command against the Redis cache:

```powershell
Get-RedisInfo
```

### Test to get Average Response Time

This example shows how to run a test against Redis to get the average response times for the cache. You need to supply a dummy Key value that will be used as a counter to increment - it's then removed after the test.

```powershell
# get average response over 120s after incrementing counter once a second
Test-RedisTiming -Key 'counter' -Seconds 120

# get average response over 120s after rapidly incrementing counter
Test-RedisTiming -Key 'counter' -Seconds 120 -NoSleep
```

### Get the value of a Key

```powershell
Get-RedisKey -Key '<some-key>'
```

### Remove all Keys that match a Pattern

This example will let you remove all keys from Redis that match a particular pattern - if the Redis version is 2.8.0 or greater then a `SCAN` is used, else `KEYS` is used.

```powershell
# remove all "user" keys
Remove-RedisKeys -Pattern 'user:*'

# remove all "user" keys, but sleep for 5s between every 1000 removed
Remove-RedisKeys -Pattern 'user:*' -SleepThreshold 1000 -SleepSeconds 5

# remove all "user" keys, up to a max of 15000 keys then stop
Remove-RedisKeys -Pattern 'user:*' -MaxDelete 15000
```

## Functions

* Add-RedisKey
* Add-RedisSetMember
* Connect-Redis
* Disconnect-Redis
* Get-RedisInfo
* Get-RedisInfoKeys
* Get-RedisKey
* Get-RedisKeyDetails
* Get-RedisKeys
* Get-RedisKeysCount
* Get-RedisKeyTTL
* Get-RedisKeyType
* Get-RedisKeyValueLength
* Get-RedisRandomKey
* Get-RedisRandomKeys
* Get-RedisRandomKeysQuick
* Get-RedisUptime
* Invoke-RedisScript
* Remove-RedisKey
* Remove-RedisKeys
* Remove-RedisSetMember
* Set-RedisIncrementKey
* Set-RedisKeyTTL
* Test-RedisTiming