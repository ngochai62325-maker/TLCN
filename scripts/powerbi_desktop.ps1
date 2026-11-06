param(
    [Parameter(Mandatory=$true)][int]$AppProcessId,
    [ValidateSet('Inspect','Capture','Invoke','SetText','Keys','Click','Paste','Maximize')][string]$Action='Inspect',
    [string]$Name,
    [string]$Value,
    [string]$OutputPath,
    [string]$InputPath,
    [int]$WindowHandle,
    [string]$Region,
    [switch]$Submit,
    [switch]$Control
)
$ErrorActionPreference='Stop'
Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
Add-Type @'
using System;
using System.Runtime.InteropServices;
public static class PbiWindow {
 [DllImport("user32.dll")] public static extern IntPtr GetForegroundWindow();
 [DllImport("user32.dll")] public static extern uint GetWindowThreadProcessId(IntPtr hWnd, out uint processId);
 [DllImport("user32.dll")] public static extern bool IsIconic(IntPtr hWnd);
 [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr hWnd);
 [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr hWnd, int cmd);
 [DllImport("user32.dll")] public static extern bool SetCursorPos(int x, int y);
 [DllImport("user32.dll")] public static extern void mouse_event(uint flags, uint dx, uint dy, uint data, UIntPtr info);
 [DllImport("user32.dll")] public static extern void keybd_event(byte key, byte scan, uint flags, UIntPtr extra);
}
'@
$appProcess=Get-Process -Id $AppProcessId
if ($appProcess.ProcessName -notmatch 'PBIDesktop') { throw 'Explicit owned Power BI Desktop process required' }
$rootCondition=New-Object System.Windows.Automation.PropertyCondition([System.Windows.Automation.AutomationElement]::ProcessIdProperty,$AppProcessId)
$appWindows=[System.Windows.Automation.AutomationElement]::RootElement.FindAll([System.Windows.Automation.TreeScope]::Children,$rootCondition)
if ($appWindows.Count -eq 0) { throw 'No Power BI window for this process' }
if ($Action -eq 'Inspect') {
    foreach ($appWindow in $appWindows) {
        $nodes=$appWindow.FindAll([System.Windows.Automation.TreeScope]::Descendants,[System.Windows.Automation.Condition]::TrueCondition)
        $results=@([pscustomobject]@{Name=$appWindow.Current.Name;Type='Window';Enabled=$appWindow.Current.IsEnabled})
        foreach ($node in $nodes) {
            if ($node.Current.Name -and -not $node.Current.IsOffscreen) { $results += [pscustomobject]@{Name=$node.Current.Name;Type=$node.Current.ControlType.ProgrammaticName;Enabled=$node.Current.IsEnabled} }
            if ($results.Count -ge 300) { break }
        }
        $results | ConvertTo-Json -Depth 3
    }
    exit
}
$targetWindow=$appWindows[$appWindows.Count-1]
if ($WindowHandle) {
    $targetWindow=[System.Windows.Automation.AutomationElement]::FromHandle([IntPtr]$WindowHandle)
    if ($targetWindow.Current.ProcessId -ne $AppProcessId) { throw 'Window must belong to the specified report process' }
} else {
    $dialogs=$targetWindow.FindAll([System.Windows.Automation.TreeScope]::Descendants, (New-Object System.Windows.Automation.PropertyCondition([System.Windows.Automation.AutomationElement]::ControlTypeProperty,[System.Windows.Automation.ControlType]::Window)))
    foreach ($dialog in $dialogs) { if ($dialog.Current.NativeWindowHandle -and $dialog.Current.IsEnabled -and -not $dialog.Current.IsOffscreen) { $targetWindow=$dialog;break } }
}
if ($Action -eq 'Maximize') { [PbiWindow]::ShowWindow([IntPtr]$targetWindow.Current.NativeWindowHandle,3) | Out-Null;exit }
if ([PbiWindow]::IsIconic([IntPtr]$targetWindow.Current.NativeWindowHandle)) { [PbiWindow]::ShowWindow([IntPtr]$targetWindow.Current.NativeWindowHandle,9) | Out-Null }
[PbiWindow]::keybd_event(18,0,0,[UIntPtr]::Zero)
[PbiWindow]::keybd_event(18,0,2,[UIntPtr]::Zero)
[PbiWindow]::SetForegroundWindow([IntPtr]$targetWindow.Current.NativeWindowHandle) | Out-Null
Start-Sleep -Milliseconds 400
[uint32]$foregroundProcess=0
[PbiWindow]::GetWindowThreadProcessId([PbiWindow]::GetForegroundWindow(),[ref]$foregroundProcess) | Out-Null
if ($foregroundProcess -ne $AppProcessId) { throw 'Report could not acquire foreground; refusing input or capture of another application' }
if ($Action -eq 'Keys') { [System.Windows.Forms.SendKeys]::SendWait($Value); exit }
if ($Action -eq 'Paste') {
    $pasteValue=if ($InputPath) { Get-Content -Raw -Encoding utf8 -LiteralPath $InputPath } else { $Value }
    if ($Name) {
        $xy=$Name.Split(',');$rect=$targetWindow.Current.BoundingRectangle
        if ($xy.Count -ne 2 -or [int]$xy[0] -lt 0 -or [int]$xy[1] -lt 0 -or [int]$xy[0] -ge $rect.Width -or [int]$xy[1] -ge $rect.Height) { throw 'Input coordinate must be inside the owned window' }
        [PbiWindow]::SetCursorPos([int]$rect.X+[int]$xy[0],[int]$rect.Y+[int]$xy[1]) | Out-Null
        [PbiWindow]::mouse_event(2,0,0,0,[UIntPtr]::Zero);[PbiWindow]::mouse_event(4,0,0,0,[UIntPtr]::Zero)
        [System.Windows.Forms.SendKeys]::SendWait('^a')
    }
    [System.Windows.Forms.Clipboard]::SetText($pasteValue);[System.Windows.Forms.SendKeys]::SendWait('^v')
    if ($Submit) { [System.Windows.Forms.SendKeys]::SendWait('{ENTER}') };exit
}
if ($Action -eq 'Click') {
    $xy=$Value.Split(',');$rect=$targetWindow.Current.BoundingRectangle
    if ($xy.Count -ne 2 -or [int]$xy[0] -lt 0 -or [int]$xy[1] -lt 0 -or [int]$xy[0] -ge $rect.Width -or [int]$xy[1] -ge $rect.Height) { throw 'Click must be inside the owned window' }
    [PbiWindow]::SetCursorPos([int]$rect.X+[int]$xy[0],[int]$rect.Y+[int]$xy[1]) | Out-Null
    if ($Control) { [PbiWindow]::keybd_event(17,0,0,[UIntPtr]::Zero) }
    try { [PbiWindow]::mouse_event(2,0,0,0,[UIntPtr]::Zero);[PbiWindow]::mouse_event(4,0,0,0,[UIntPtr]::Zero) }
    finally { if ($Control) { [PbiWindow]::keybd_event(17,0,2,[UIntPtr]::Zero) } }
    exit
}
if ($Action -eq 'Capture') {
    if (-not $OutputPath) { throw 'OutputPath required' }
    $rect=$targetWindow.Current.BoundingRectangle
    if ($Region) {
        $parts=$Region.Split(',') | ForEach-Object {[int]$_}
        if ($parts.Count -ne 4 -or $parts[0] -lt 0 -or $parts[1] -lt 0 -or $parts[2] -lt 1 -or $parts[3] -lt 1 -or $parts[0]+$parts[2] -gt $rect.Width -or $parts[1]+$parts[3] -gt $rect.Height) { throw 'Capture region must stay inside the owned report window' }
        $rect=[System.Windows.Rect]::new(($rect.X+$parts[0]),($rect.Y+$parts[1]),$parts[2],$parts[3])
    }
    $bmp=New-Object System.Drawing.Bitmap([int]$rect.Width,[int]$rect.Height)
    $graphics=[System.Drawing.Graphics]::FromImage($bmp)
    $graphics.CopyFromScreen([int]$rect.X,[int]$rect.Y,0,0,$bmp.Size)
    $bmp.Save($OutputPath,[System.Drawing.Imaging.ImageFormat]::Png)
    $graphics.Dispose();$bmp.Dispose();exit
}
$nameCondition=New-Object System.Windows.Automation.PropertyCondition([System.Windows.Automation.AutomationElement]::NameProperty,$Name)
$matches=@()
foreach ($appWindow in $appWindows) {
    foreach ($item in $appWindow.FindAll([System.Windows.Automation.TreeScope]::Descendants,$nameCondition)) {
        if (-not $item.Current.IsOffscreen -and $item.Current.IsEnabled) {
            if ($Action -eq 'Invoke' -and -not ($item.GetSupportedPatterns().Id -contains [System.Windows.Automation.InvokePattern]::Pattern.Id)) { continue }
            $matches += $item
        }
    }
}
if ($matches.Count -ne 1) { throw "Expected one enabled visible control '$Name', found $($matches.Count)" }
if ($Action -eq 'Invoke') { $pattern=$matches[0].GetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern);$pattern.Invoke() }
elseif ($Action -eq 'SetText') { $pattern=$matches[0].GetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern);$pattern.SetValue($Value) }
