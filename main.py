
from flet import *
import flet_fastapi
import GUILoginScreen as GLS
import GUILandingPage as GLP
from fastapi.responses import StreamingResponse
import pandas as pd
import SYSConnectToServers as CS
from io import BytesIO

TableData = pd.DataFrame()
ReportName = ''

class MainPage(Container):

    def __init__(self):
        super().__init__()

        self.ODS = CS.ConnectToODSServer()     

    # file select for desktop apps only

    def generate_excel(self):
        excel_buffer = BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
            TableData.to_excel(writer, index=False, sheet_name='Sheet1')
            worksheet = writer.sheets['Sheet1']
            worksheet.set_zoom(100)
        excel_buffer.seek(0)
        return excel_buffer

app = flet_fastapi.FastAPI()

@app.get('/download')

async def download():
    excel_data = MainPage().generate_excel()
    return StreamingResponse(excel_data, 
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={ReportName}.xlsx"})

# call login page 

async def main(page: Page):

    async def download_file(e):
        await page.launch_url_async(url='/download', web_window_name='_self')

    page.theme_mode = "dark"
    UserName = 'm.ahmed' 
    await page.add_async(GLS.LoginScreen(page, download_file).LoginScreen)
    # await page.add_async(GLP.LandingPage(page, download_file, UserName).HomeScreen)
    await page.update_async()

app.mount("/", flet_fastapi.app(main))