# Как использовать продукт? 
Склонируйте репозиторий 
```
$ git clone https://github.com/kostyamyasso2002/SoCPC-hackaton
```
Соберите docker образ
```
$ docker build -t socpc-image SoCPC-hackaton/
```
Запустите docker file
```
$ docker run --mount src=$(pwd)/data",target="/data/",type=bind --cpus 4 -m 8000M --name socpc-image 
```
Подождать когда image соберется. Полученные данные будут лежать в docker container по адресу /data
Чтобы их скопировать можно воспользоваться командой:
```
$ docker cp --name SoCPC-image:data/out_cm.json .
```

# Что мы проделали по ходу работы.

2.1 Создание voice_traffic и data_traffic
Стек: Apache Spark, Python
Описание: Здесь особо без идей: просто с помощью 2 исходных файлов собираем другие 2 по заданным формулам.
Время работы: 10 секунд

2.2 Создание client_profile
Стек: Apache Spark, Python
Описание: тут тоже просто из 2 полученных бд создаем итоговую, отсорченную по месяцам.
Время работы:

2.3 Загнать все в Aerospike
Статус: Не осилили и решили вместо него использовать примитивные способы хранения данных
Время работы: медленно

3.1 Статус: Сделано
Стек: Kafka, Python
Описание: складываем два json в два топика у Apache Kafka.

3.2 Статус: в процессе
Стек: Kafka, Python
Описание: Проходимся по двум топикам двумя указателями в хронологическом порядке, храним в сете все айдишники, которым пришло уведомление. Пока что просто, если айдишника не было, смотрим на графу “client”: если там web - ставим sms, если mobile, то push. И добавляем это в топик. 

4.1 В конце комбинируем все data frames в одну json табличку с названием out_cm.json

