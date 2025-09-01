# without shortcut
# Create the main application entry
$appPath = "HKLM:\SOFTWARE\Amazon\AppStream\Applications"
$appName = "IBD-Annotation"
$fullPath = "$appPath\$appName"

# Remove existing entry if it exists
if (Test-Path $fullPath) {
    Remove-Item $fullPath -Recurse -Force
}

# Create new entry with proper structure
New-Item -Path $fullPath -Force
Set-ItemProperty -Path $fullPath -Name "DisplayName" -Value "IBD Medical Imaging Annotation"
Set-ItemProperty -Path $fullPath -Name "Executable" -Value "C:\Scripts\ibd_app_launch_user_assignment.bat"
Set-ItemProperty -Path $fullPath -Name "WorkingDirectory" -Value "C:\Scripts"
Set-ItemProperty -Path $fullPath -Name "IconPath" -Value ""

Write-Host "Application registered with correct structure!"


# with shortcut:
# Create shortcut in All Users Start Menu
$shortcutPath = "C:\ProgramData\Microsoft\Windows\Start Menu\Programs\IBD Medical Imaging Annotation.lnk"
$WScript = New-Object -ComObject WScript.Shell
$shortcut = $WScript.CreateShortcut($shortcutPath)
$shortcut.TargetPath = "C:\Scripts\ibd_app_launch_user_assignment.bat"
$shortcut.WorkingDirectory = "C:\Scripts"
$shortcut.Description = "IBD Medical Imaging Annotation"
$shortcut.Save()

Write-Host "Start Menu shortcut created!"