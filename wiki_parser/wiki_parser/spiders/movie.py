import scrapy
import pandas as pd
import requests
import gzip
from io import StringIO,BytesIO

class MovieSpider(scrapy.Spider):
    name = 'movie'
    allowed_domains = ['ru.wikipedia.org']

    def __init__(self, *args, **kwargs):
        super(MovieSpider, self).__init__(*args, **kwargs)
        
        #Загрузка и подготовка датасета с рейтингами
        response = requests.get('https://datasets.imdbws.com/title.ratings.tsv.gz')
        with gzip.open(BytesIO(response.content), 'rb') as f:
            file_content = f.read().decode('utf-8')
        self.df = pd.read_csv(StringIO(file_content), sep='\t')
        self.df['tconst'] = self.df['tconst'].str.slice(start = 2, step = 1)
        self.df = self.df[['tconst', 'averageRating']]
        
    
    def start_requests(self):
        URL = 'https://ru.wikipedia.org/w/index.php?title=Категория:Фильмы_по_алфавиту'
        headers = {
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:45.0) Gecko/20100101 Firefox/45.0'
        }# Добавил на всякий случай первый из списка юзерагентов
        yield scrapy.Request(url=URL, callback=self.movie_parser, meta=headers)

    def movie_parser(self, response):
        
        for selector in response.css('#mw-pages .mw-category-columns a::attr(href)'):
            print(selector.extract())
            yield response.follow(selector.extract(), callback=self.page_parser)
        
        
        next_page = response.css('#mw-pages > a:contains("Следующая страница")::attr(href)').extract_first()
        if next_page:
            yield response.follow(next_page, callback=self.movie_parser)

    def page_parser(self, response):
        
        # join в связке с методом xpath string() позволяютвытягивать текст даже из под дучерних тегов
        # А это часто необходимо
        id = ''.join(response.css('span[data-wikidata-property-id="P345"]').xpath('string()').extract())[3:]
        rait = self.df[self.df['tconst'] == id]['averageRating'].values#Вытягиваю данные о рейтинге

        yield {
            'title': ' '.join(response.css('#firstHeading').xpath("string()").extract()),
            'genre': ' '.join(response.css('span[data-wikidata-property-id="P136"]').xpath('string()').extract()),
            'director': ' '.join(response.css('span[data-wikidata-property-id="P57"]').xpath('string()').extract()),
            'country': ' '.join(response.css('span[data-wikidata-property-id="P495"]').xpath('string()').extract()),
            'year': ' '.join(response.css('.dtstart, span[data-wikidata-property-id="P577"]').xpath('string()').extract()),
            'id': id,
            'rating': rait[0] if rait > 0 else None #костыль от ошибок
            
        }