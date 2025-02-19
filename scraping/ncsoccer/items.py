# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class NcsoccerItem(scrapy.Item):
    league_name = scrapy.Field()
    game_date = scrapy.Field()
    game_time = scrapy.Field()
    home_team = scrapy.Field()
    away_team = scrapy.Field()
    field = scrapy.Field()
    facility_id = scrapy.Field()
    league_id = scrapy.Field()
