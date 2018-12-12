# Deployment script pushing lib and documentation to python feed

function Get-ScriptDirectory
{
    $Invocation = (Get-Variable MyInvocation -Scope 1).Value
    Split-Path $Invocation.MyCommand.Path
}

if (!(Test-Path variable:global:OctopusReleaseNumber)) {
    Write-Error "OctopusReleaseNumber variable must be set"
    return
}
if (!(Test-Path variable:global:PyPiLocation)) {
    Write-Error "PyPiLocation variable must be set"
    return
}
if (!(Test-Path variable:global:DocLocation)) {
    Write-Error "DocLocation variable must be set"
    return
}

$Root = (Get-ScriptDirectory)
$PythonSource = Join-Path $Root "*.whl"
$PythonDocSource = Join-Path $Root "docs"

Write-Host "Copying Python library from $PythonSource to $PyPiLocation"
& "xcopy" "$PythonSource" "$PyPiLocation" /Y /I

Write-Host "Copying Python documentation from $PythonDocSource to $DocLocation"
& "$AzCopyTool" /Z:azjournal /V:azlog.txt /Source:"$PythonDocSource" /Dest:"$DocLocation" /DestKey:"$DocLocationKey" /S /Y /SetContentType

Write-Host "Done copying package files";
