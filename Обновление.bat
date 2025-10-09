@echo off

echo Получаем последнее обновление...
del ".git/index.lock"
git reset --hard
git pull origin main

echo Готово!
pause