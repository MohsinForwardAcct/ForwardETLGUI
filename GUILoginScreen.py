
from flet import *
import bcrypt as bc
import SYSConnectToServers as CS
import GUILandingPage as GLP
import re


class LoginScreen(Container):

    def __init__(self, page, download):
        super().__init__()
        
        self.page = page
        self.Download = download
        self.ODS = CS.ConnectToODSServer()
        self.salt = bc.gensalt()
        self.GetLoginScreen()
        self.LoginScreen =  Container(expand=True,content=Row(controls=[(Column([self.username, self.password, self.signup, self.submit],
                            alignment=MainAxisAlignment.CENTER))],alignment=MainAxisAlignment.CENTER))

    # input fields for login and pswd 

    def GetLoginScreen(self):
        self.username = TextField(border_color="#AD1457", label='Username',color='white', text_align=TextAlign.LEFT, height=40, width=200)
        self.password = TextField(border_color="#AD1457", label='Password', color='white', text_align=TextAlign.LEFT,height=40, width=200,
                        password=True, can_reveal_password=True, on_change=self.GetInputStatus)
        self.signup = Container(height=20)
        self.submit = ElevatedButton(text='Login', bgcolor="#AD1457", color='white', height=40, width=200, on_click=self.GetSubmit, disabled=True)

    async def GetInputStatus(self, e):
        if all([self.username.value, self.password.value]): self.submit.disabled = False
        else: self.submit.disabled = True
        await self.LoginScreen.update_async()

    def GetAlertMessage(self, Highlight, Commentry):
        self.AlertMessage = AlertDialog(title=Text(Highlight,weight="bold",color="#AD1457"),content=Text(Commentry),
            actions=[TextButton("Ok",on_click=self.GetAlertMessageClose,style=ButtonStyle(shape={"":RoundedRectangleBorder(radius=6)},
            bgcolor="#AD1457",color="white"))],actions_alignment=MainAxisAlignment.END,on_dismiss=[])
        
    async def GetAlertMessageClose(self,e):
        await self.LoginScreen.page.close_dialog_async()

    async def GetPsswdSetupScreen(self):
        self.GetSetPsswdScreen()
        self.LoginScreen.content.controls.clear()
        self.LoginScreen.content.controls.append(self.UpdatePsswdScreen)
        await self.LoginScreen.page.update_async()

    def GetSetPsswdScreen(self):
        self.NewPsswd = TextField(border_color="#AD1457", label='New Password',color='white', text_align=TextAlign.LEFT, height=40, width=200)
        self.ConfirmPsswd = TextField(border_color="#AD1457", label='Confirm Password',color='white', text_align=TextAlign.LEFT, height=40, width=200, 
                                      on_change=self.GetValidation)
        self.UpdatePsswd = ElevatedButton(text='Update Password', bgcolor="#AD1457", color='white', height=30, width=160, on_click=self.GetUpdatePsswd)
        self.PsswdRules = Container(width=200, content=Text('Password Criteria: ',text_align='left',size=15))
        self.PsswdCriteria = Container(width=1000, content=Text(self.GetPsswdCriteria(),text_align='left',size=15))
        self.UpdatePsswdScreen =  Container(expand=True,padding=100,content=Column(controls=[(Column([self.NewPsswd, self.ConfirmPsswd, self.UpdatePsswd],
        alignment=MainAxisAlignment.START)),(Column([Container(height=50), self.PsswdRules, self.PsswdCriteria],alignment=MainAxisAlignment.START))]))
        
    def GetPsswdCriteria(self):
        PsswdCriteria = f"Your new password must contatin Minimum 8 Characters of which 1 uppercase letter, 1 lowercase letter, 1 numeric and 1 special character"
        return PsswdCriteria
    
    async def GetValidation(self, e):
        Validation = self.GetPsswdCheck()
        if Validation != None: self.GetAlertMessage("New Password Setup Failed", Validation); await self.LoginScreen.page.show_dialog_async(self.AlertMessage)          
        
    def GetPsswdCheck(self):
        if len(self.NewPsswd.value) < 8: return 'Password must be at least 8 characters long.'
        if not re.search(r"[A-Z]", self.NewPsswd.value): return 'Password must contain at least one uppercase letter.'
        if not re.search(r"[a-z]", self.NewPsswd.value): return 'Password must contain at least one lowercase letter.'
        if not re.search(r"\d", self.NewPsswd.value): return 'Password must contain at least one number.'
        if not re.search(r"[!@#$%^&*(){}><,.\|:']", self.NewPsswd.value): return 'Password must contain at least one uppercase letter.'
        return None

    async def GetUpdatePsswd(self, e):
        if self.NewPsswd.value != self.ConfirmPsswd.value: 
            self.GetAlertMessage("New Password Setup Failed","New Psswd & Confirm Psswd do not match")
            await self.LoginScreen.page.show_dialog_async(self.AlertMessage)   
        else: 
            password = self.ConfirmPsswd.value.encode("utf-8"); hashedpsswd = bc.hashpw(password, bc.gensalt()); hashedpsswd = hashedpsswd.decode("utf-8")
            self.ODS.qryODSAppendData(f"Update ETLUserData Set HashedPassword = '{hashedpsswd}' where UserName = '{self.username.value}'")
            self.GetAlertMessage("New Password Setup","New Password Updated Successfully!")
            await self.LoginScreen.page.show_dialog_async(self.AlertMessage)   
            await self.GetBackLoginScreen()


    async def GetBackLoginScreen(self):
        self.GetLoginScreen()
        self.BackLoginScreen =  Container(expand=True,content=Row(controls=[(Column([self.username, self.password, self.signup, self.submit],
                            alignment=MainAxisAlignment.CENTER))],alignment=MainAxisAlignment.CENTER))
        self.LoginScreen.content.controls.clear()
        self.LoginScreen.content.controls.append(self.BackLoginScreen)
        await self.LoginScreen.page.update_async()


    # pswd validation     

    async def GetSubmit(self, e):
        query = f"select HashedPassword from ETLUserData where UserName = '{self.username.value}'"
        StoredPsswd = str(self.ODS.qryODSGetData(query).values)
        if StoredPsswd == '[]': self.GetAlertMessage("Login Failed","User Not Setup"); await self.LoginScreen.page.show_dialog_async(self.AlertMessage)
        else: 
            StoredPsswd = str(self.ODS.qryODSGetData(query).values[0][0]);self.ODS.ODSConnection.close
            StoredPsswd = StoredPsswd.encode("utf-8")
            SetupPsswd = 'forward@123'; SetupPsswd = SetupPsswd.encode("utf-8")
            EnteredPsswd = self.password.value.encode("utf-8")
            if bc.checkpw(SetupPsswd, StoredPsswd) and bc.checkpw(EnteredPsswd, StoredPsswd): await self.GetPsswdSetupScreen() 
            elif bc.checkpw(EnteredPsswd, StoredPsswd): await self.PageRoute()
            else: self.GetAlertMessage("Login Failed","Username or Password is Incorrect!"); await self.LoginScreen.page.show_dialog_async(self.AlertMessage)

    # route page to landingpage

    async def PageRoute(self):
        UserName = self.username.value
        await self.page.clean_async()
        await self.page.go_async('/ForwardReporting')
        await self.page.add_async(GLP.LandingPage(self.page, self.Download, UserName).HomeScreen)
        await self.page.update_async()
