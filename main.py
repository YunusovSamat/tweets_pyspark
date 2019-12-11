import pyspark


class TweetsSpark:
    def __init__(self):
        self.tweets = None

    # Метод для записи в переменную, выборочные данные из файла.
    def set_tweets_data(self, path):
        sc =pyspark.SparkContext.getOrCreate()
        # Чтение данных и разбиение на колонки.
        self.tweets = sc.textFile(path).map(
            lambda line: line[1:-1].split('","'))
        # Запись выборочных колонок.
        self.tweets = self.tweets.map(lambda row: (row[1], row[10], row[24]))

    # Метод для фильтрации данных.
    def set_tweets_filter(self, lang='ru'):
        # Фильтрация только зарубежных пользователей и корректное
        # количество ответов.
        self.tweets = self.tweets.filter(
            lambda row: row[1] != lang and row[2].isdigit())
        # Преобразование колонки "количество ответов" в числовой тип.
        self.tweets = self.tweets.map(lambda row: (row[0], int(row[2])))

    # Метод для получения userid, который набрал наибольшее 
    # количество ответов.
    def get_userid_max_rc(self):
        return self.tweets.max(lambda row: row[1])[0]

    # Метод для вывода отсортированного списка твитов. 
    def get_sorted_data(self, count=10, asc=False):
        # Сортировка колонки "количество ответов" по убываниею.
        data_sorted = self.tweets.sortBy(lambda row: row[1], ascending=asc)
        return data_sorted.take(count)


if __name__ == '__main__':
    file_path = 'file:///home/samat/Downloads/ira_tweets_csv_hashed.csv'
    ts = TweetsSpark()
    ts.set_tweets_data(file_path)
    ts.set_tweets_filter()
    print(ts.get_userid_max_rc())


