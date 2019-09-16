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
        $Data
    )

    if ($PSCmdlet.ParameterSetName -eq "Key"){
        $value = Get-RedisKey -Key $Key
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