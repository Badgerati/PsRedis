Task 'Build' SERedis, { }

Task 'SERedis' {
    if (Test-Path ./src/lib/Redis) {
        Remove-Item -Path ./src/lib/Redis -Force -Recurse -ErrorAction Stop | Out-Null
    }

    if (Test-Path ./temp) {
        Remove-Item -Path ./temp -Force -Recurse -ErrorAction Stop | Out-Null
    }

    $version = '1.2.6'
    nuget install stackexchange.redis -source nuget.org -version $version -outputdirectory ./temp | Out-Null
    New-Item -Path ./src/lib/Redis -ItemType Directory -Force | Out-Null
    Copy-Item -Path "./temp/StackExchange.Redis.$($version)/lib/*" -Destination ./src/lib/Redis -Recurse -Force | Out-Null

    if (Test-Path ./temp) {
        Remove-Item -Path ./temp -Force -Recurse | Out-Null
    }

    Remove-Item -Path ./src/lib/Redis/net46 -Force -Recurse | Out-Null
}