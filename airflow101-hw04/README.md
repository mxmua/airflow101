### 4. Ветвление и общение DAG между собой

#### Challenge

В этом задании мы апгрейдим то, что получилось во второй домашке.

1. В начало пайпа добавляем оператор, который проверяет, что PostgreSQL для хранения результата доступна.
1. В случае если все ок – едем дальше по пайпу, если нет – скипаем все дальнейшие шаги.
1. Добавляем sanity-check оператор, который проверяет, что данные в порядке и их можно класть в хранилище.
1. В случае если все ок с данными – кладем в PostgreSQL, если нет – шлем уведомление в наш любимый
   чат в Телеге о том, что все плохо.

Требования к DAG:

- "доступность" PostgreSQL довольно растяжимое понятие – решайте сами, что вы в него вкладываете, только напишите об этом;
- при алертах в телегу нужно также передавать task_id и dag_id, но брать их из контекста, а не ручками;
- _очевидно_, что операторы, которые выполняют проверку условий в данном задании должны быть экземплярами наследников
  класса BaseBranchOperator.
----

#### Решение

За основу взят код DAG-а с домашки №2 с доработками:
- Шаблоны SQL вынесены в файл **hw04_sql_templates.py** 
- Новый код домашки №4 вынесен в **hw04_utils.py** 

1. Доступность PostgreSQL проверяется подключением к серверу, с помощью хука.
Параметром включается проверка на вставку в таблицу результата.
1. Sanity-check организован отдельным оператором, который рисует подробный отчет о проблемах.
1. В Телеграм прилетает сразу отчет с указанием, где именно лажа в данных.

###### Что проверяет шаг sanity-check
- ключевые поля (natural key) источников - уникальны
- ключевые поля (natural key) источников - ожидаемого типа
- значение в поле количество конвертируется в целочисленный тип (мы ж его умножать собираемся)
- price конвертируется в число с плавающей точкой

P.S. По-хорошему еще нужно бы валидировать:  

- в заказах нет товаров, которые отсутствуют в справочнике товаров
- почекать, конвертируются ли значения полей с датами в дату
- отсутствие дат из будущего
- в благородных домах видел еще и проверку email-ов со всеми этими DNS лукапами, корректировкой опечаток и т.п.
   