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