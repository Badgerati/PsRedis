$Global:PsRedisCacheConnection = $null
$Global:PsRedisServerConnection = $null
$Global:PsRedisDatabaseIndex = 0
$Global:PsRedisRoot = (Split-Path -Parent -Path $MyInvocation.MyCommand.Path)

function Add-RedisDll
{
    [CmdletBinding()]
    param ()

    Add-Type -Path (Join-Path $Global:PsRedisRoot '..\packages\StackExchange.Redis.1.2.6\lib\net45\StackExchange.Redis.dll') -ErrorAction Stop | Out-Null
}
