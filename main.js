const { app, BrowserWindow } = require('electron');
const path = require('path');

function createWindow() {
  const win = new BrowserWindow({
    width: 1000,
    height: 800,
    webPreferences: {
      // セキュリティ設定（ローカルアプリなので nodeIntegration を有効にするか、fetch を使う）
      nodeIntegration: true,
      contextIsolation: false,
    },
  });

  win.loadFile('index.html');
  
  // 開発中はデバッグツールを開く（不要ならコメントアウト）
  // win.webContents.openDevTools();
}

app.whenReady().then(createWindow);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit();
});