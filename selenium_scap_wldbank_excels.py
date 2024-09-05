import time
import os
import glob
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

download_dir = r"C:\worldbank"

# Set up Chrome options to change the default download directory
options = webdriver.ChromeOptions()
prefs = {
    "download.default_directory": download_dir,  # Set the download directory
    "download.prompt_for_download": False,       # Disable the download prompt
    "directory_upgrade": True,                   # Allow directory upgrades
    "safebrowsing.enabled": True                 # Disable safe browsing to prevent download blocking
}
options.add_experimental_option("prefs", prefs)

# Set up the WebDriver
driver_path = r'C:\webdrivers\chromedriver.exe'  # Update this with the path to your WebDriver
service = Service(driver_path)
driver = webdriver.Chrome(service=service, options=options)

# Maximize the browser window
driver.maximize_window()

def download_data():
    driver.get('https://www.worldbank.org/en/projects-operations/procurement')
    wait = WebDriverWait(driver, 20)
    
    try:
        download_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.accordion-download[data-type="excel"]')))
        download_button.click()
        time.sleep(2)

        top_values = [1, 250, 500, 750, 1000]
        # bottom_values = [1, 50, 100]
        bottom_values = [1, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950]

        download_count = 1  # Initialize download count

        for top_value in top_values:
            for bottom_value in bottom_values:
                print(f"Moving top slider to {top_value}")
                move_slider_to_value(0, 'ng5-slider-pointer-min', top_value)  # Move the top slider first

                print(f"Moving bottom slider to {bottom_value}")
                move_slider_to_value(1, 'ng5-slider-pointer-min', bottom_value)  # Then move the bottom slider
                
                # Click the "Ok" button to start the download
                ok_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[text()="Ok"]')))
                ok_button.click()
                time.sleep(5)  # Adjust time based on download size and network speed

                # # Rename the most recently downloaded file
                # rename_latest_download(download_count)  # Use download count for naming
                # download_count += 1  # Increment download count

                # Reopen the modal after each download
                reopen_download_modal(wait)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        driver.quit()

def move_slider_to_value(slider_index, slider_class, target_value):
    """Moves the specified slider to the given value."""
    try:
        sliders = driver.find_elements(By.CLASS_NAME, slider_class)

        if slider_index >= len(sliders):
            print(f"Slider index {slider_index} is out of range.")
            return

        slider = sliders[slider_index]

        # Perform slider movement by offset
        action = ActionChains(driver)
        action.click_and_hold(slider).move_by_offset(target_value, 0).release().perform()
        time.sleep(1)

    except Exception as e:
        print(f"Error moving slider to value {target_value}: {e}")

# def rename_latest_download(file_number):
#     """Renames the most recently downloaded file with a sequential number."""
#     try:
#         # Wait for the file to appear in the download directory
#         time.sleep(2)
#         list_of_files = glob.glob(f'{download_dir}/*.xls*')  # Adjust this for the correct file extension

#         if list_of_files:
#             latest_file = max(list_of_files, key=os.path.getctime)
#             new_filename = os.path.join(download_dir, f'{file_number}.xlsx')
            
#             # Check if the new file name already exists
#             if os.path.exists(new_filename):
#                 print(f"File {new_filename} already exists. Skipping renaming.")
#             else:
#                 os.rename(latest_file, new_filename)
#                 print(f"Renamed file to: {new_filename}")
#         else: 
#             print("No files found to rename.")
#     except Exception as e:
#         print(f"Error renaming file: {e}")

def reopen_download_modal(wait):
    """Reopens the download modal after setting sliders or downloading."""
    try:
        # Check if the download button is available, indicating the modal might be closed
        download_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'a.accordion-download[data-type="excel"]')))
        download_button.click()
        time.sleep(2)
    except Exception as e:
        print(f"Error reopening download modal: {e}")

# Run the download process
download_data()
