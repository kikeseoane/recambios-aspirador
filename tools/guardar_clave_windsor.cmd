@echo off
title Guardar clave de Windsor.ai para Grow
powershell.exe -NoProfile -Command "$ErrorActionPreference='Stop'; $secret=Read-Host 'Pega la clave API de Windsor.ai y pulsa Intro' -AsSecureString; if ($secret.Length -lt 8) { throw 'La clave esta vacia o es demasiado corta.' }; $pointer=[Runtime.InteropServices.Marshal]::SecureStringToBSTR($secret); try { $value=[Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer); [Environment]::SetEnvironmentVariable('GROWTH_WINDSOR_API_KEY',$value,'User'); Write-Host 'Clave guardada para tu usuario de Windows.' } finally { [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer) }"
if errorlevel 1 echo No se pudo guardar la clave. Copia el error y avisame, sin incluir la clave.
pause
