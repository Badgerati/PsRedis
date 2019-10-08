$path = $MyInvocation.MyCommand.Path
$src = (Split-Path -Parent -Path $path) -ireplace '[\\/]tests[\\/]unit', '/src/'
Get-ChildItem "$($src)/*.ps1" -Recurse | Resolve-Path | ForEach-Object { . $_ }

Describe 'Get-RedisKeyValueLengthPrivate' {
    Context 'Key'{
        It 'String'{
            Mock Get-RedisKey {"Testing"} -Verifiable

            Get-RedisKeyValueLengthPrivate -Key "key" | Should Be 7

            Assert-MockCalled -CommandName Get-RedisKey -Times 1 -Scope It
        }

        It 'Array'{
            Mock Get-RedisKey {@("Testing", "SecondValue")} -Verifiable

            Get-RedisKeyValueLengthPrivate -Key "key" | Should Be 18

            Assert-MockCalled -CommandName Get-RedisKey -Times 1 -Scope It
        }
    }
    Context 'Data'{
        It 'String'{
            Get-RedisKeyValueLengthPrivate -Data "Testing" | Should Be 7
            Get-RedisKeyValueLengthPrivate -Data "" | Should Be 0
            Get-RedisKeyValueLengthPrivate -Data " " | Should Be 1
        }
        It 'Array'{
            Get-RedisKeyValueLengthPrivate -Data @() | Should Be 0
            Get-RedisKeyValueLengthPrivate -Data @("Testing") | Should Be 7
            Get-RedisKeyValueLengthPrivate -Data @("Testing", "SecondValue") | Should Be 18
        }
    }
}

Describe 'Test-RedisIsConnected'{
    It 'Connected'{
        (Test-RedisIsConnected -Connection @{"IsConnected" = $true}) | Should Be $true
    }

    It 'Not Connected'{
        (Test-RedisIsConnected -Connection @{"IsConnected" = $false}) | Should Be $false
    }

    It 'Null'{
        Test-RedisIsConnected -Connection $null | Should Be $false
    }
}

Describe 'Get-RedisConnection'{
    It 'Error'{
        $Global:PsRedisServerConnection = $null

        {Get-RedisConnection} | Should Throw "No Redis connection has been initialized"
    }
}

Describe 'Get-RedisDatabase'{
    It 'Error'{
        $Global:PsRedisCacheConnection = $null

        {Get-RedisDatabase} | Should Throw "No Redis connection has been initialized"

        $Global:PsRedisCacheConnection = @{"IsConnected" = $false}

        {Get-RedisDatabase} | Should Throw "No Redis connection has been initialized"
    }
}
