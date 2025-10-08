@echo off

echo Получаем последнее обновление...
git reset --hard
git pull origin main

echo Готово!
pause