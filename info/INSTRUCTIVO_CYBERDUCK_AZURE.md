# Conectar CyberDuck a Azure Storage (ADLS Gen2)

## Contexto

- **Storage Account:** `stsynapsemetadata`
- **Tipo:** ADLS Gen2 (HNS habilitado)
- **Container destino:** `raw-csvs`
- **URL:** `https://stsynapsemetadata.blob.core.windows.net/raw-csvs/`

> **Nota:** ADLS Gen2 es retrocompatible con el endpoint Blob (`blob.core.windows.net`) para operaciones basicas de upload/download. CyberDuck usa este endpoint y funciona correctamente para transferir archivos.

---

## Metodo 1: Account Key (recomendado por simplicidad)

### Paso 1 — Obtener la Account Key

```powershell
az storage account keys list `
  --account-name stsynapsemetadata `
  --resource-group Analitica-Proyecto `
  --query "[0].value" -o tsv
```

Copiar el valor resultante.

### Paso 2 — Configurar conexion en CyberDuck

1. Abrir CyberDuck
2. Click en **"Open Connection"** (o `Ctrl+O`)
3. En el dropdown de protocolo (parte superior), seleccionar: **"Microsoft Azure Storage"**
4. Llenar los campos:
   - **Server:** `stsynapse.blob.core.windows.net`
   - **Username:** `stsynapse`
   - **Password:** XXXXXX
5. Click en **"Connect"**

### Paso 3 — Navegar al container

Una vez conectado, veras la lista de containers del storage account. Hacer doble click en **`raw-csvs`** para entrar.

---

## Metodo 2: SAS Token

> **Advertencia:** CyberDuck tiene un [bug conocido](https://github.com/iterate-ch/cyberduck/issues/12707) con SAS tokens a nivel de **contenedor**. Se requiere un SAS token a nivel de **cuenta** (`ss=b&srt=co`).

### Paso 1 — Habilitar el perfil SAS en CyberDuck

1. Ir a **Edit > Preferences > Profiles** (o `Ctrl+,` > Profiles)
2. Buscar y habilitar: **"Azure (Shared Access Signature Token)"**
3. Cerrar Preferences

### Paso 2 — Generar SAS token a nivel de cuenta

```powershell
az storage account generate-sas `
  --account-name stsynapsemetadata `
  --permissions rwdlacup `
  --resource-types sco `
  --services b `
  --expiry 2026-03-16 `
  --https-only `
  -o tsv
```

### Paso 3 — Configurar conexion

1. Click en **"Open Connection"**
2. Seleccionar protocolo: **"Azure (Shared Access Signature Token)"**
3. Llenar los campos:
   - **Server:** `stsynapsemetadata.blob.core.windows.net`
   - **Password:** sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2026-02-13T01:25:15Z&st=2026-02-12T17:10:15Z&spr=https&sig=1VysXfbU2ju39k7WE7Pg6kVgbDJ2j8r6Xi7e63a%2BB7I%3D
4. Click en **"Connect"**

### Paso 4 — Navegar al container

Igual que el Metodo 1: doble click en **`raw-csvs`**.

---

## Operaciones comunes

### Subir archivos

- Drag & drop desde el explorador de archivos hacia la ventana de CyberDuck
- O click derecho > **Upload...**

### Descargar archivos

- Seleccionar archivo(s) > click derecho > **Download** (o `Alt+Enter` para descargar al directorio default)

### Crear carpeta

- Click derecho > **New Folder...**

---

## Limitaciones de CyberDuck con ADLS Gen2

| Aspecto | Estado |
|---------|--------|
| Upload/download basico | Funciona via endpoint Blob |
| SAS token de contenedor | NO funciona (bug #12707) |
| Gestion de ACLs (ADLS) | No soportado |
| Endpoint DFS (`dfs.core.windows.net`) | No soportado |
| Operaciones jerarquicas HNS | No soportado |

Para operaciones avanzadas (ACLs, permisos de directorio), usar **Azure Storage Explorer** en su lugar.

---

## Alternativa: Azure Storage Explorer

Si CyberDuck presenta problemas, Azure Storage Explorer tiene soporte nativo para ADLS Gen2:

1. Descargar desde: https://azure.microsoft.com/features/storage-explorer/
2. Abrir > **Add an account** > **Storage account or service** > **Account name and key**
3. Ingresar: Display name, Account name (`stsynapsemetadata`), Account key
4. Navegar a `raw-csvs`

Ventaja: soporta endpoint DFS + ACLs + Azure AD.
