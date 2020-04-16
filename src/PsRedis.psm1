# root path to module
$root = Split-Path -Parent -Path $MyInvocation.MyCommand.Path

# get the path to the libraries and load them
$libraries = Join-Path $root 'lib'
$libraries = Join-Path $libraries 'Redis'
$name = 'StackExchange.Redis.dll'

switch ($PSEdition.ToLowerInvariant()) {
    'core' {
        $path = Join-Path 'netstandard1.5' $name
    }

    default {
        $path = Join-Path 'net45' $name
    }
}

$path = (Join-Path $libraries $path)
[System.Reflection.Assembly]::LoadFrom($path) | Out-Null

# load private functions
Get-ChildItem "$($root)/Private/*.ps1" | Resolve-Path | ForEach-Object { . $_ }

# get current functions to import public functions
$sysfuncs = Get-ChildItem Function:

# load public functions
Get-ChildItem "$($root)/Public/*.ps1" | Resolve-Path | ForEach-Object { . $_ }

# get functions from memory and compare to existing to find new functions added
$funcs = Get-ChildItem Function: | Where-Object { $sysfuncs -notcontains $_ }

# export the module's public functions
Export-ModuleMember -Function ($funcs.Name)
