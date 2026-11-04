# OmniGet Subtitulos 0.14.0 — edición para compartir

## Cambios de esta edición

- Permite elegir subtítulos en español disponibles en Udemy, incluidas variantes regionales y pistas automáticas cuando la opción correspondiente está activada.
- Integra los subtítulos en el MP4 con texto blanco y fondo oscuro. El resultado es un solo archivo de video.
- Conserva el video original si falla o se cancela la conversión.
- Obtiene FFmpeg y FFprobe al procesar un video con subtítulos si aún no están instalados.
- Desactiva las actualizaciones automáticas de la edición oficial para que no sustituyan esta versión personalizada.

## Para quien instala

1. Descarga el instalador de Windows de esta edición y ejecútalo. El instalador no está firmado digitalmente.
2. Abre **OmniGet Subtitulos**, inicia sesión en Udemy con tu propia cuenta y elige español como idioma de subtítulos en la configuración del curso.
3. La primera integración de subtítulos puede descargar FFmpeg y FFprobe automáticamente y tardar más. Las siguientes usan las herramientas ya instaladas.

La aplicación solo puede integrar pistas que Udemy ofrezca para cada clase. No traduce subtítulos. Los cursos, contraseñas, cookies y sesiones del autor de esta edición no forman parte del instalador ni del código fuente.

## Para generar el instalador desde el código fuente

Se necesitan Windows, Node.js, Rust con MSVC, las herramientas de compilación de Visual Studio y las dependencias del proyecto. Consulta también el apartado **Build from source** de [README.md](README.md). En PowerShell, desde la raíz del repositorio:

```powershell
corepack pnpm install --frozen-lockfile
$env:VITE_OMNIGET_FRIEND_BUILD = '1'
corepack pnpm build
node node_modules/@tauri-apps/cli/tauri.js build --config src-tauri/tauri.friend.conf.json --bundles nsis --ci --no-sign
```

El instalador se crea en `src-tauri/target/release/bundle/nsis/`. La variable de entorno solo afecta a la compilación de la interfaz: evita ofrecer actualizaciones del proyecto oficial dentro de esta edición.

Esta edición deriva de OmniGet y conserva su licencia **GNU GPLv3**. Al distribuir el instalador, comparte también el código fuente correspondiente y el archivo [LICENSE](LICENSE). No incluyas las carpetas locales de datos de OmniGet, cursos descargados ni archivos `.env`.
