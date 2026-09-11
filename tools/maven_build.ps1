$ErrorActionPreference = 'Stop'
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
# Choose the installed modern compiler for --release 11; guest runtime uses Java 11.
$env:JAVA_HOME = 'C:\Program Files\Java'
& $taskMaven -B -q -f (Join-Path $taskRoot 'warehouse\flink\pom.xml') package
if ($LASTEXITCODE -ne 0) { throw "Maven build failed: $LASTEXITCODE" }
