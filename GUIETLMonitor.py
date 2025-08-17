
from flet import *
from datetime import datetime
import SYSConnectToServers as CS


class ETLMonitor(Container):

    def __init__(self, page):
        super().__init__(expand= True, bgcolor= "#17181d", border_radius= 10)

        self.page = page
        self.ODS = CS.ConnectToODSServer()
        self.TimeStamp = datetime.now()
        self.Testing = ElevatedButton(text="Testing", bgcolor="#1f2128", color="#64DD17", scale=transform.Scale(0.8),data=False,
                    on_click=[])
        self.header =  Container(expand=True, height=60, bgcolor="#AD1457",border_radius=border_radius.only(top_left=15,top_right=15),
                    padding=padding.only(left=15,right=15),content=Row(expand=True,alignment=MainAxisAlignment.SPACE_BETWEEN,
                    controls=[Container(content=Text("ETL Monitor",size=20,color="white",weight="bold")),self.Testing]))          
        self.table = DataTable(expand= True, columns=[DataColumn(Text("ETL Progress....", weight="bold", color="#AD1457", size=18)),
                    DataColumn(Text("Status", weight="bold", color="#AD1457", size=18),numeric=True)],width=1200, heading_row_height= 75,
                    data_row_max_height= 50)        
        self.content = Container(expand=True,content=(Column(expand=True,horizontal_alignment="center",controls=[Divider(color="transparent"),
                    Container(width= 1200,content=Row(alignment="center", controls=[self.header])),
                    Container(expand=True, width= 1200, padding=  10, border_radius= border_radius.only(top_left=10, top_right=10),
                    shadow= BoxShadow(spread_radius=8, blur_radius=15, color=colors.with_opacity(0.15, "black"),offset=Offset(4,4)),
                    bgcolor=colors.with_opacity(0.75, "#1f2128"),content=Column(expand=True,scroll="hidden",controls=[self.table]))])))


