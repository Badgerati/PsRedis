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