# websurfer2000З
## проверить структуру проекта!
- websurfer2000
- -- analizator
- -- db
-   ---- temp
- -- parser
## Запуск
надо находиться в корневой папке
python parser/parser.py {здесь url без / в конце} {здесь таймаут в секундах, дефолт 120}
ждем пока отработает, затем:
python analizator/analizator.py {здесь тот же url}
