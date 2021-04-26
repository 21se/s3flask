chcp 1251
rem Настройка сервера
	rem Настроим псевдоним доступа admincred
mc config host add admincred http://localhost:5000 minioadmin minioadmin
	rem Создадм бакеты(корневые каталоги)
mc mb admincred/user1bucket
mc mb admincred/user2bucket
	rem создадим пользователей
mc admin user add admincred user1 Qwerty123
mc admin user add admincred user2 Qwerty123
	rem создадим политику доступа из файла
mc admin policy add admincred user1-policy user1-policy.json
mc admin policy add admincred user2-policy user2-policy.json
	rem применим политики доступа к пользователям
mc admin policy set admincred user1-policy user=user1
mc admin policy set admincred user2-policy user=user2
	rem Выведем список пользователей и их политики
mc admin user list admincred 

rem Примеры клиента
	rem Настроим псевдоним доступа user1cred, user2cred
mc config host add user1cred http://localhost:9000 user1 Qwerty123
mc config host add user2cred http://localhost:9000 user2 Qwerty123
	rem Выведем список файлов(второй Access Denied.)
mc ls user1cred/user1bucket
mc ls user2cred/user1bucket
	rem Вывести свойства файла
mc stat user1cred/user1bucket/file1.txt
	rem Скопируем\переместим файл из S3 на локальный компьютер
mc cp user1cred/user1bucket/file1.txt file1.txt
mc mv user1cred/user1bucket/file11.txt file11.txt
	rem Удалим файл S3
mc rm user1cred/user1bucket/file1.txt
	rem Прочитаем содержимое файла из S3
mc cat user1cred/user1bucket/file1.txt
	rem Прочитаем содержимое файла из S3, первые 2 строки
mc head user1cred/user1bucket/file1.txt -n 2
	rem Посчитаем размер каталога S3
mc du user1cred/user1bucket
	rem Сравним 2 каталога
mc diff user1cred/user1bucket/  user2cred/user2bucket/
	rem Создать ссылку на файл, доступную без аутентификации 4 часа
mc share download --expire 4h user1cred/user1bucket/file1.txt

pause