
from flet import *
from GUIETLMonitor import *
import GUILogicTables as LT
import GUIBordereaux as Brdx
import GUIVariableList as GVL
import GUILoginScreen as GLS

class LandingPage(Container):

    def __init__(self, page, download, username):
        super().__init__()

        self.page = page
        self.Download = download
        self.UserName = username
        self.MainInterFace = Container(expand=True,bgcolor=colors.WHITE,col = {"xxl": 10.2,"xl": 9.8,"lg": 9,"md": 8,"sm": 7,"xs": 6},
                            content=Column(controls=[]))
        self.HomeScreen = ResponsiveRow(expand=True,alignment= MainAxisAlignment.CENTER,
                        controls=[self.GetNavInterFace(self.GetNavigationBar(),"Forward Reporting"), self.MainInterFace])
        
    # adjust screen size  

    def GetNavInterFace(self, InterFace, NavTitle):
        NavigationInterFace = Container(expand=True,bgcolor=colors.BLACK,alignment=alignment.center,
                        col = {"xxl": 1.8,"xl": 2.2,"lg": 3,"md": 4,"sm": 5,"xs": 6},content=Column(expand=True,alignment=MainAxisAlignment.CENTER,
                        controls=[self.GetNavTitle(NavTitle), Divider(height=2,color="#AD1457"),Column(expand=True,controls=[InterFace])]))
        return NavigationInterFace
    
    # Navigation title bar
    
    def GetNavTitle(self, NavTitle):
        NavTitle = Container(height=80, expand=False, alignment=alignment.center,content=Text(NavTitle,size=24, color=colors.PINK_ACCENT_700, weight='bold'))
        return NavTitle
    
    # highlight nav buttons when hover
    
    async def HighLight(self,e):
        if e.data == 'true': await self.UpdateHighLight(e, 'white10')
        else: e.control.bgcolor = await self.UpdateHighLight(e, None)

    async def UpdateHighLight(self,e,con):
        e.control.bgcolor = con
        await e.control.update_async()    

    # refresh screen 

    async def UpdateScreen(self, MainScreen):
        onscreen = len(self.MainInterFace.content.controls)
        if onscreen > 0: self.MainInterFace.content.controls.clear()
        self.MainInterFace.content.controls.append(MainScreen)
        await self.page.update_async()

# pages work in progress

    def GetDevelopment(self):
        DevelpmentPage = Container(expand= True, bgcolor=colors.BLACK, content=Text('Page under development',text_align='center',size=150))
        return DevelpmentPage

    # main screen view

    def GetMainScreen(self, Content):
        MainScreen = Container(expand= True, bgcolor=colors.BLACK, border_radius=2, padding=10, content=Content)
        return MainScreen
    
    # Navigations buttons to handle screen changes

    async def GetMainInterFace(self, e):
        LabelName = e.control.content.controls[1].value
        IconName = e.control.content.controls[0].icon
        if LabelName == "ETL Monitor": MainScreen = Column(expand=True,controls=[ETLMonitor(self.page)]); await self.UpdateScreen(MainScreen)
        elif LabelName == "Variable List": MainScreen = self.GetMainScreen(GVL.GUIVariableList(self.page, self.Download, self.UserName).ReportingScreen); await self.UpdateScreen(MainScreen)
        elif LabelName == "Logic Tables": MainScreen = self.GetMainScreen(LT.LogicTables(self.page, self.Download, self.UserName).ReportingScreen); await self.UpdateScreen(MainScreen)
        elif LabelName == "Bordereaux": MainScreen = self.GetMainScreen(Brdx.Bordereaux(self.page, self.Download, self.UserName).ReportingScreen); await self.UpdateScreen(MainScreen)
        elif LabelName == "UserList": MainScreen = self.GetMainScreen(GVL.GUIVariableList(self.page, 'ETLUserData').ReportingScreen); await self.UpdateScreen(MainScreen)     
        elif LabelName == "System Logs": MainScreen = self.GetMainScreen(Column(controls=[self.GetDevelopment()])); await self.UpdateScreen(MainScreen) 
        elif LabelName == "Administrator": MainScreen = self.GetMainScreen(Column(controls=[self.GetDevelopment()])); await self.UpdateScreen(MainScreen) 
        elif LabelName == "Logout": await self.GetBackLoginScreen()
    
    def GetNavButtons(self, IconName:str, LabelName:str):
        NavButtons = Container(width=250,height=45,border_radius=10,padding=padding.only(left=8), on_hover=self.HighLight,    
                               on_click=self.GetMainInterFace,    
            content=Row(controls=[IconButton(icon=IconName,icon_size=25,icon_color=colors.PINK_ACCENT_700,
            style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=7)},overlay_color={"":'transparent'})),
            Text(value=LabelName,color=colors.PINK_ACCENT_700,size=15,opacity=1,weight='bold',animate_opacity=200)]))
        return NavButtons
    
    # Create Navigation buttons

    def GetNavigationBar(self):  
        HomeNavigationBar =  Container(expand=True, padding=padding.only(top=10),alignment=alignment.top_center,
                content=Column(expand=True, controls=[Column(alignment=MainAxisAlignment.START,horizontal_alignment='center',
                controls=  [self.GetNavButtons(icons.MONITOR_OUTLINED, "ETL Monitor"),
                self.GetNavButtons(icons.PLAYLIST_ADD_OUTLINED,"Variable List"),          
                self.GetNavButtons(icons.TABLE_ROWS_OUTLINED,"Logic Tables"),
                self.GetNavButtons(icons.ANALYTICS_OUTLINED,"Bordereaux"),
                self.GetNavButtons(icons.REPORT_OUTLINED,"System Logs"),
                self.GetNavButtons(icons.BEENHERE_OUTLINED,"Validation"),
                self.GetNavButtons(icons.ADMIN_PANEL_SETTINGS_OUTLINED,"Administrator")]),
                Container(expand=True, padding=padding.only(top=10),alignment=alignment.bottom_left,
                content=Column(alignment=MainAxisAlignment.END,horizontal_alignment='left',
                controls=[Divider(height=5,color='#AD1457'),self.GetNavButtons(icons.PERSON,self.UserName),self.GetNavButtons(icons.LOGOUT_ROUNDED,"Logout")]))]))
        return HomeNavigationBar
    
    async def GetBackLoginScreen(self):
        await self.GetAlertMessage('Logout Confirmation','Confirm Logout?, Any unsaved changes will be lost!')
           

    async def GetAlertMessageAction(self,e):
        self.HomeScreen.controls.clear()
        self.HomeScreen.controls.append(GLS.LoginScreen(self.page, self.Download).LoginScreen)
        await self.HomeScreen.page.close_dialog_async()
        await self.HomeScreen.page.update_async()
        

    # handle alert dialtog close events 

    async def GetAlertMessageClose(self,e):
        print('im closed')
        await self.HomeScreen.page.close_dialog_async()


    async def GetAlertMessage(self, Highlight, Commentry):
        self.AlertMessage = AlertDialog(title=Text(Highlight,weight="bold",color="#AD1457"),content=Text(Commentry),
            actions=[TextButton("Yes",on_click=self.GetAlertMessageAction,style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},bgcolor="#AD1457",color="#FAF8F9")),
                     TextButton("No",on_click=self.GetAlertMessageClose,style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},bgcolor="#AD1457",color="#FAF8F9")),],
            actions_alignment=MainAxisAlignment.END,on_dismiss=[])
        await self.HomeScreen.page.show_dialog_async(self.AlertMessage)