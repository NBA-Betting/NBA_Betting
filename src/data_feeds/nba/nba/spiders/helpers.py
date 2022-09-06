import datetime


def return_season_dates(year):
    """Returns needed date info for spiders based on year.

    Args:
        year (int): End year of season.

    Returns:
        dict: Date info. Structure:
        {"season_years": season_years,
        "start_day": start_day,
        "final_day": last_day,
        "final_month": last_month,
        "final_year": last_year}
    """
    seasons = {
        "2022": {
            "abbrv": "2021-22",
            "start": "October 19, 2021",
            "end": "April 10, 2022",
        },
        "2021": {
            "abbrv": "2020-21",
            "start": "December 22, 2020",
            "end": "May 16, 2021",
        },
        "2020": {
            "abbrv": "2019-20",
            "start": "October 22, 2019",
            "end": "August 14, 2020",
        },
        "2019": {
            "abbrv": "2018-19",
            "start": "October 16, 2018",
            "end": "April 10, 2019",
        },
        "2018": {
            "abbrv": "2017-18",
            "start": "October 17, 2017",
            "end": "April 11, 2018",
        },
        "2017": {
            "abbrv": "2016-17",
            "start": "October 25, 2016",
            "end": "April 12, 2017",
        },
        "2016": {
            "abbrv": "2015-16",
            "start": "October 27, 2015",
            "end": "April 13, 2016",
        },
        "2015": {
            "abbrv": "2014-15",
            "start": "October 28, 2014",
            "end": "April 15, 2015",
        },
        "2014": {
            "abbrv": "2013-14",
            "start": "October 29, 2013",
            "end": "April 16, 2014",
        },
        "2013": {
            "abbrv": "2012-13",
            "start": "October 30, 2012",
            "end": "April 17, 2013",
        },
        "2012": {
            "abbrv": "2011-12",
            "start": "December 25, 2011",
            "end": "April 26, 2012",
        },
        "2011": {
            "abbrv": "2010-11",
            "start": "October 26, 2010",
            "end": "April 13, 2011",
        },
        "2010": {
            "abbrv": "2009-10",
            "start": "October 27, 2009",
            "end": "April 14, 2010",
        },
    }
    start_date = datetime.datetime.strptime(seasons[str(year)]["start"],
                                            "%B %d, %Y")
    final_date = datetime.datetime.strptime(seasons[str(year)]["end"],
                                            "%B %d, %Y")
    season_years = seasons[str(year)]["abbrv"]
    last_day = final_date.day
    last_month = final_date.month
    last_year = final_date.year
    return_dict = {
        "season_years": season_years,
        "start_date": start_date,
        "final_date": final_date,
        "final_day": last_day,
        "final_month": last_month,
        "final_year": last_year,
    }
    return return_dict

    date_mask_2022 = (
        full_dataset['game_date'] >= pd.to_datetime("October 19, 2021")) & (
            full_dataset['game_date'] <= pd.to_datetime("April 10, 2022"))
