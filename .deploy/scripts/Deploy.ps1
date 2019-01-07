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
if (!(Test-Path variable:global:PyPiUser)) {
    Write-Error "PyPiUser variable must be set"
    return
}
if (!(Test-Path variable:global:PyPiPassword)) {
    Write-Error "PyPiUser variable must be set"
    return
}
if (!(Test-Path variable:global:DocLocation)) {
    Write-Error "DocLocation variable must be set"
    return
}

$Root = (Get-ScriptDirectory)
$PythonDocSource = Join-Path $Root "docs"

# Create/activate python environment to work with
Write-Host "Creating/updating deployment environment '$PythonEnvironment'"
& "conda env update -f deployenv.yml"
Write-Host "Activating deployment environment '$PythonEnvironment'"
& "activate $PythonEnvironment"

if ($PyPiRepositoryUrl) {
    # Upload to specific repository
    Write-Host "Uploading to $PyPiRepositoryUrl"
    & "twine upload --repository-url $PyPiRepositoryUrl * -u $PyPiUser -p $PyPiPassword"
}
else {
    Write-Host "Uploading to default repository"
    & "twine upload * -u $PyPiUser -p $PyPiPassword"
}

Write-Host "Copying Python documentation from $PythonDocSource to $DocLocation"
& "$AzCopyTool" /Z:azjournal /V:azlog.txt /Source:"$PythonDocSource" /Dest:"$DocLocation" /DestKey:"$DocLocationKey" /S /Y /SetContentType

Write-Host "Done copying package files";
