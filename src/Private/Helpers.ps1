function Test-RedisIsConnected
{
    [CmdletBinding()]
    param (
        [Parameter()]
        $Connection
    )

    return (($null -ne $Connection) -and ($Connection.IsConnected))
}

function Get-RedisKeyValueLengthPrivate
{
    [CmdletBinding()]
    param (
        [Parameter(ParameterSetName="Key")]
        [string]
        $Key,

        [Parameter(ParameterSetName="Data")]
        $Data,

        [Parameter()]
        $ConnectionName
    )

    if ($PSCmdlet.ParameterSetName -eq "Key"){
        $value = Get-RedisKey -Key $Key -ConnectionName $ConnectionName
    }
    else{
        $value = $Data
    }

    $length = $value.Length

    if ($value -is 'array') {
        $length = 0
        ($value | ForEach-Object { $length += $_.Length })
    }

    return $length
}

function Get-RedisDatabase
{
    [CmdletBinding()]
    param(
        [Parameter()]
        $ConnectionName
    )

    if ([string]::IsNullOrWhiteSpace($ConnectionName)){
        $ConnectionName = "__default__"
    }

    $cacheConnection = $Global:PsRedisCacheConnections[$ConnectionName]

    if (!(Test-RedisIsConnected $cacheConnection)) {
        throw "No Redis connection has been initialized"
    }

    return $cacheConnection.GetDatabase($Global:PsRedisDatabaseIndex)
}

function Get-RedisConnection
{
    [CmdletBinding()]
    param(
        [Parameter()]
        $ConnectionName
    )

    if ([string]::IsNullOrWhiteSpace($ConnectionName)){
        $ConnectionName = "__default__"
    }

    $serverConnection = $Global:PsRedisServerConnections[$ConnectionName]

    if ($null -eq $serverConnection) {
        throw "No Redis connection has been initialized"
    }

    return $serverConnection
}