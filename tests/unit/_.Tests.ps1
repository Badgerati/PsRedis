$path = $MyInvocation.MyCommand.Path
$src = (Split-Path -Parent -Path $path) -ireplace '[\\/]tests[\\/]unit', '/src/'
$sysfuncs = Get-ChildItem Function:
Get-ChildItem "$($src)/*.ps1" -Recurse | Resolve-Path | ForEach-Object { . $_ }
$funcs = Get-ChildItem Function: | Where-Object { $sysfuncs -notcontains $_ }

Describe 'PowerShell Syntax' {
    It 'Verifies all functions start with correct tag' {
        $invalid = $funcs.Name | Where-Object { $_ -inotmatch '^[a-z]+\-Redis[a-z0-9]+$' }
        if ($invalid.Length -gt 0) {
            $invalid | Foreach-Object { Write-Host "> $($_)" -ForegroundColor Red }
        }

        $invalid.Length | Should Be 0
    }

    It 'Verifies all functions use a valid Verb' {
        $verbs = (Get-Verb).Verb

        $invalid = $funcs.Name | Where-Object { $verbs -inotcontains ($_ -split '-')[0] }
        if ($invalid.Length -gt 0) {
            $invalid | Foreach-Object { Write-Host "> $($_)" -ForegroundColor Red }
        }

        $invalid.Length | Should Be 0
    }

    It 'Verifies all functions use CmdletBinding' {
        $invalid = $funcs.Name | Where-Object { (Get-Command $_).Definition -inotmatch '^\s+\[CmdletBinding\(' }
        if ($invalid.Length -gt 0) {
            $invalid | Foreach-Object { Write-Host "> $($_)" -ForegroundColor Red }

        }

        $invalid.Length | Should Be 0
    }
}