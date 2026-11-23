$ErrorActionPreference = 'Stop'
if ($env:JAVA_HOME) {
    $taskJavac = Join-Path $env:JAVA_HOME 'bin\javac.exe'
    if (-not (Test-Path -LiteralPath $taskJavac -PathType Leaf)) {
        throw 'JAVA_HOME must point to a JDK installation.'
    }
} elseif (-not (Get-Command javac.exe -ErrorAction SilentlyContinue)) {
    throw 'Install JDK 11 or later and set JAVA_HOME or add its bin directory to PATH.'
}
$taskRoot = Split-Path -Parent $PSScriptRoot
$taskRuntime = Join-Path $taskRoot 'runtime\build-tools'
$taskVersion = '3.9.9'
$taskArchive = Join-Path $taskRuntime "apache-maven-$taskVersion-bin.zip"
$taskUrl = "https://archive.apache.org/dist/maven/maven-3/$taskVersion/binaries/apache-maven-$taskVersion-bin.zip"
New-Item -ItemType Directory -Force -Path $taskRuntime | Out-Null
if (-not (Test-Path -LiteralPath $taskArchive)) { Invoke-WebRequest -UseBasicParsing -Uri $taskUrl -OutFile $taskArchive }
$taskExpected = ((Invoke-WebRequest -UseBasicParsing -Uri "$taskUrl.sha512").Content.Trim() -split '\s+')[0]
if ((Get-FileHash -LiteralPath $taskArchive -Algorithm SHA512).Hash.ToLower() -ne $taskExpected.ToLower()) { throw 'Maven archive checksum mismatch' }
$taskMaven = Join-Path $taskRuntime "apache-maven-$taskVersion\bin\mvn.cmd"
if (-not (Test-Path -LiteralPath $taskMaven)) { Expand-Archive -LiteralPath $taskArchive -DestinationPath $taskRuntime }
& $taskMaven -B -q -f (Join-Path $taskRoot 'warehouse\flink\pom.xml') package
if ($LASTEXITCODE -ne 0) { throw "Maven build failed: $LASTEXITCODE" }
