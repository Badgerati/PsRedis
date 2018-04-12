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

## Examples

Before running any of the examples, ensure you have imported the PsRedis module from the `src` directory:

```powershell
Import-Module .\PsRedis.psm1
```

> Each of the functions have a `-Close` switch, when supplied PsRedis will close the connection after running the function, else it will leave it open

### Return INFO about an Instance

This example will return the information you get from running the `INFO` command against the Redis instance:

```powershell
Get-RedisInfo -Connection '<host>:<port>' -Close
```

### Test to get Average Response Time

This example will should you how to run a test against Redis to get the average response times from Redis. YOu need to supply a dummy Key value, that will be used as a counter to increment - it's removed after the test.

```powershell
# get average response over 120s after incrementing counter once a second
Test-RedisTimings -Connection '<host>:<port>' -Key 'counter' -Seconds 120 -Close

# get average response over 120s after rapidly incrementing counter
Test-RedisTimings -Connection '<host>:<port>' -Key 'counter' -Seconds 120 -NoSleep -Close

# get average response over 120s after incrementing counter once a second, recreating the connection every attempt
Test-RedisTimings -Connection '<host>:<port>' -Key 'counter' -Seconds 120 -Reconnect -Close
```

### Get the value of a Key

```powershell
Get-RedisKey -Connection '<host>:<port>' -Key '<some-key>' -Close
```

### Remove all Keys that match a Pattern

This example will let you remove all keys from Redis that match a particular pattern - if the Redis version is 2.8.0 or greater, then a SCAN is used, else KEYS is used.

```powershell
# remove all "user" keys
Remove-RedisKeys -Connection '<host>:<port>' -Pattern 'user:*' -Close

# remove all "user" keys, but sleep for 5s between every 1000 removed
Remove-RedisKeys -Connection '<host>:<port>' -Pattern 'user:*' -SleepThreshold 1000 -SleepSeconds 5 -Close

# remove all "user" keys, up to a max of 15000 keys then stop
Remove-RedisKeys -Connection '<host>:<port>' -Pattern 'user:*' -MaxDelete 15000 -Close
```

## Other Functions

* Get-RedisInfo
* Get-RedisInfoKeys
* Get-RedisKey
* Set-RedisKey
* Get-RedisKeys
* Get-RedisKeysCount
* Remove-RedisKeys
* Test-RedisTimings