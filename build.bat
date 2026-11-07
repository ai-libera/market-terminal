@echo off
rem Builds every Go program and every Qt app of this repo into build\bin\.
setlocal enabledelayedexpansion
pushd "%~dp0"

set MAKE=mingw32-make
where cl >nul 2>nul && set MAKE=nmake

if not exist build\bin mkdir build\bin

for /d %%D in (*) do (
  if /i not "%%D"=="build" if /i not "%%D"=="common" (
    findstr /m /b /c:"package main" "%%D\*.go" >nul 2>nul
    if not errorlevel 1 (
      echo go build %%D
      go build -o "build\bin\%%D.exe" ".\%%D"
      if errorlevel 1 goto :fail
    )
  )
)

for /d %%D in (*) do (
  if exist "%%D\%%D.pro" (
    echo qmake %%D
    if not exist "build\qmake\%%D" mkdir "build\qmake\%%D"
    pushd "build\qmake\%%D"
    qmake "..\..\..\%%D\%%D.pro"
    if errorlevel 1 ( popd & goto :fail )
    !MAKE!
    if errorlevel 1 ( popd & goto :fail )
    popd
  )
)

popd
exit /b 0

:fail
set ERR=%errorlevel%
popd
exit /b %ERR%
