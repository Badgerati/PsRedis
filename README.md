# PsRedis

PowerShell module for using Redis, via the StackExchange.Redis library.

> Requires PowerShell 5+ and NuGet CLI to work

## Setup

After cloning the Repo, run following in PowerShell:

```powershell
cd <path-to-repo>
choco install nuget.commandline -y
.\nuget-restore.ps1
cd .\src\
```

## Usage

Before running any of the examples, ensure you have imported the PsRedis module:

```powershell
Import-Module .\src\PsRedis.psm1
```

You scripts must first open a connection, and then close it:

```powershell
Connect-Redis -ConnectionString $ConnectionString

# logic

Disconnect-Redis
```

### Return INFO about an Instance

This example will return the information you get from running the `INFO` command against the Redis instance:

```powershell
Get-RedisInfo
```

### Test to get Average Response Time

This example will should you how to run a test against Redis to get the average response times from Redis. You need to supply a dummy Key value, that will be used as a counter to increment - it's removed after the test.

```powershell
# get average response over 120s after incrementing counter once a second
Test-RedisTimings -Key 'counter' -Seconds 120

# get average response over 120s after rapidly incrementing counter
Test-RedisTimings -Key 'counter' -Seconds 120 -NoSleep
```

### Get the value of a Key

```powershell
Get-RedisKey -Key '<some-key>'
```

### Remove all Keys that match a Pattern

This example will let you remove all keys from Redis that match a particular pattern - if the Redis version is 2.8.0 or greater then a SCAN is used, else KEYS is used.

```powershell
# remove all "user" keys
Remove-RedisKeys -Pattern 'user:*'

# remove all "user" keys, but sleep for 5s between every 1000 removed
Remove-RedisKeys -Pattern 'user:*' -SleepThreshold 1000 -SleepSeconds 5

# remove all "user" keys, up to a max of 15000 keys then stop
Remove-RedisKeys -Pattern 'user:*' -MaxDelete 15000
```

## Functions

* Disconnect-Redis
* Get-RedisConnection
* Get-RedisDatabase
* Get-RedisInfo
* Get-RedisInfoKeys
* Get-RedisKey
* Get-RedisKeys
* Get-RedisKeysCount
* Get-RedisKeyTTL
* Get-RedisKeyType
* Get-RedisKeyValueLength
* Get-RedisRandomKey
* Get-RedisUptime
* Connect-Redis
* Remove-RedisKey
* Remove-RedisKeys
* Set-RedisIncrementKey
* Set-RedisKey
* Set-RedisKeyTTL
* Test-RedisIsConnected
* Test-RedisTimings