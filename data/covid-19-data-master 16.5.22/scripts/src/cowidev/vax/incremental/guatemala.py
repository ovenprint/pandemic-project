import re

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from cowidev.utils.clean import clean_count, clean_date
from cowidev.vax.utils.incremental import increment


def main():

    data = {
        "location": "Guatemala",
        "source_url": "https://gtmvigilanciacovid.shinyapps.io/3869aac0fb95d6baf2c80f19f2da5f98",
        "vaccine": "Moderna, Oxford/AstraZeneca",
    }

    op = Options()
    op.add_argument("--headless")
    with webdriver.Chrome(options=op) as driver:
        driver.maximize_window()  # For maximizing window
        driver.implicitly_wait(20)  # gives an implicit wait for 20 seconds
        driver.get(data["source_url"])
        driver.find_element_by_class_name("fa-syringe").click()
        date = driver.find_element_by_class_name("logo").text
        dose1 = driver.find_element_by_id("dosisaplicadas1").find_element_by_tag_name("h3").text
        dose2 = driver.find_element_by_id("dosisaplicadas2").find_element_by_tag_name("h3").text
        dose3 = driver.find_element_by_id("dosis_refuerzo").find_element_by_tag_name("h3").text
        doses = driver.find_element_by_id("dosisaplicadasTot").find_element_by_tag_name("h3").text

    data["people_vaccinated"] = clean_count(dose1)
    data["people_fully_vaccinated"] = clean_count(dose2)
    data["total_vaccinations"] = clean_count(doses)
    data["total_boosters"] = clean_count(dose3)

    date = re.search(r"\d+/\d+/202\d", date).group(0)
    data["date"] = clean_date(date, "%d/%m/%Y")

    increment(
        location=data["location"],
        total_vaccinations=data["total_vaccinations"],
        people_vaccinated=data["people_vaccinated"],
        people_fully_vaccinated=data["people_fully_vaccinated"],
        total_boosters=data["total_boosters"],
        date=data["date"],
        source_url=data["source_url"],
        vaccine=data["vaccine"],
    )
