import { Component, OnDestroy, signal } from '@angular/core';
import { LeafletDirective } from '@bluehalo/ngx-leaflet';
import * as L from 'leaflet';
import type { FeatureCollection, Point } from 'geojson';

// ---------------------------------------------------------------------------
// TIPOS: describen la forma de los datos que devuelve la API (para que TypeScript avise si nos equivocamos)
// ---------------------------------------------------------------------------
type Properties = {
  mag: number | null; magType?: string | null; place: string; time: number; updated: number;
  country_id?: string | null; country_name?: string | null;
};
type Filters = { start: string | null; end: string | null; min_mag: number | null; country_id: string | null };
type Meta = {
  generated?: number; last_ingestion?: number | null; count?: number; truncated?: boolean;
  total?: number; mag_null_excluded?: number; filters?: Filters;
  mode?: string; data_range?: { min: number; max: number } | null;   // mode: 'live' o 'replay'
};
type Events = FeatureCollection<Point, Properties> & { metadata?: Meta };
type Config = { mode: 'demo' | 'api'; apiUrl: string; refreshMs: number; skipNgrokWarning: boolean };

type Tramo = { tramo: string; n: number };
type Stats = {
  metadata: { generated: number; last_ingestion: number | null; total_eventos: number; aviso: string; mode: string };
  por_hora: { aviso: string | null; datos: { hora: string; n: number; parcial: boolean }[] };
  por_pais: { sin_pais_asignado: number; otros: { n_paises: number; n: number };
              datos: { country_id: string | null; country_name: string; n: number }[] };
  magnitud: { sin_valor: number; nota: string; datos: Tramo[] };
  profundidad: { sin_valor: number; datos: Tramo[] };
  comparacion_historica: {
    unidad: string; ultimas_24h: number; ultimas_24h_sin_magnitud: number; historico_total: number;
    media_diaria_historica: number | null; ratio: number | null; aviso: string;
    periodo_historico: { dias: number }; filtros: { country_id: string | null; min_mag: number };
  };
};
// Una fila de la tabla de eventos (ya con los textos listos para mostrar)
type Fila = { id: string; fecha: string; mag: string; prof: string; pais: string; lugar: string };

const STALE_MS = 5 * 60 * 1000;   // si la ultima ingesta de USGS tiene mas de 5 minutos, se avisa de datos desactualizados

@Component({
  selector: 'app-root', standalone: true, imports: [LeafletDirective],
  templateUrl: './app.component.html', styleUrl: './app.component.css'
})
export class AppComponent implements OnDestroy {
  // ---- Estado que se pinta en pantalla. Un "signal" es una variable que avisa a la pagina cuando cambia ----
  readonly mode = signal('Cargando configuración');
  readonly status = signal('Preparando el mapa…');
  readonly lastRead = signal('Sin lectura');           // ultima vez que el navegador consulto la API con exito
  readonly generated = signal('Sin información');      // hora a la que la API genero esa respuesta
  readonly lastIngestion = signal('Sin información');  // ultima consulta correcta a USGS (la hace el productor)
  readonly stale = signal<string | null>(null);        // aviso de datos desactualizados
  readonly backendError = signal<string | null>(null); // aviso de backend caido
  readonly periodText = signal('Sin consulta');
  readonly replay = signal(false);                     // true cuando se esta viendo la REPRODUCCION historica (no datos reales)
  readonly truncatedMsg = signal<string | null>(null);
  readonly count = signal(0);
  readonly omitted = signal(0);
  readonly countriesStatus = signal('Cargando catálogo');
  readonly countries = signal<{ id: string; name: string }[]>([]);  // para el desplegable de paises
  readonly stats = signal<Stats | null>(null);
  readonly statsError = signal<string | null>(null);
  readonly rows = signal<Fila[]>([]);
  readonly shown = signal(100);                        // cuantas filas de la tabla se ven

  readonly options: L.MapOptions = { center: L.latLng(18, 0), zoom: 2, minZoom: 2, maxZoom: 8, preferCanvas: true, worldCopyJump: false };

  private map?: L.Map;
  private events = L.layerGroup();
  private timer?: ReturnType<typeof setTimeout>;
  private controller = new AbortController();
  private destroyed = false;
  private config?: Config;
  private seq = 0;   // numero de consulta: si una respuesta lenta llega tarde, se ignora y no pisa a la nueva
  // Filtros actuales (los mismos se aplican al mapa, a la tabla y a los graficos)
  private fStart = ''; private fEnd = ''; private fMag = ''; private fCountry = ''; private fSource = 'live';

  async onMapReady(map: L.Map): Promise<void> {
    this.map = map;
    const base = L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 19, noWrap: true, attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
    }).addTo(map);
    const borders = L.geoJSON(undefined, { style: { color: '#334155', weight: 1, fill: false },
      onEachFeature: (feature, layer) => {
        const name = document.createElement('span');
        name.textContent = feature.properties?.NAME_ES ?? feature.properties?.ADMIN ?? 'Sin nombre';
        layer.bindTooltip(name);
      }
    }).addTo(map);
    this.events.addTo(map);
    L.control.layers({ 'OpenStreetMap': base }, { 'Países (Natural Earth)': borders, 'Terremotos': this.events }).addTo(map);
    map.attributionControl.addAttribution('Fronteras: Natural Earth · Datos reales: USGS');
    try {
      const catalogo = await this.readJson<FeatureCollection>('datos/paises.geojson');
      borders.addData(catalogo);
      // Lista de paises para el filtro: id (ADM0_A3) y nombre en espanol, ordenados alfabeticamente
      this.countries.set(catalogo.features
        .map(f => ({ id: String(f.properties?.['ADM0_A3'] ?? ''), name: String(f.properties?.['NAME_ES'] ?? f.properties?.['ADMIN'] ?? '') }))
        .filter(c => c.id && c.name)
        .sort((a, b) => a.name.localeCompare(b.name, 'es')));
      this.countriesStatus.set('Catálogo de países cargado');
    } catch { this.countriesStatus.set('No se pudo cargar el catálogo de países'); }
    if (this.destroyed) return;
    try {
      const config = await this.readJson<Config>('config.json');
      if (!['demo', 'api'].includes(config.mode)) throw new Error('Modo inválido');
      this.config = config;
      this.mode.set(config.mode === 'demo' ? 'DEMOSTRACIÓN · datos ficticios' : 'API · datos del backend');
      await this.refresh();
    } catch { this.status.set('Error: revisa public/config.json'); }
  }

  // Boton "Aplicar filtros": guarda los valores del formulario y consulta de nuevo ya mismo
  apply(start: string, end: string, minMag: string, country: string, source: string): void {
    if (start && end && start > end) { this.status.set('La fecha inicial es posterior a la final: corrige el rango.'); return; }
    this.fStart = start; this.fEnd = end; this.fMag = minMag.trim(); this.fCountry = country;
    this.fSource = source === 'replay' ? 'replay' : 'live';   // solo dos valores posibles
    this.shown.set(100);
    clearTimeout(this.timer);
    void this.refresh();
  }
  more(): void { this.shown.update(n => n + 100); }

  private async readJson<T>(url: string, headers: Record<string, string> = {}): Promise<T> {
    const response = await fetch(url, { headers, cache: 'no-store',
      signal: AbortSignal.any([this.controller.signal, AbortSignal.timeout(15000)]) });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return response.json() as Promise<T>;
  }
  // Igual que readJson pero nunca lanza error: devuelve {data} o {error}. Asi el mapa y los graficos fallan por separado.
  private async tryJson<T>(url: string, headers: Record<string, string>): Promise<{ data?: T; error?: string }> {
    try { return { data: await this.readJson<T>(url, headers) }; }
    catch (e) { return { error: e instanceof Error ? e.message : 'error' }; }
  }
  // Traduce el error tecnico a una frase que entienda quien mira el panel
  private explain(error?: string): string {
    if (error === 'HTTP 404') return 'No hay datos de reproducción todavía: ejecuta primero la reproducción histórica';
    if (error === 'HTTP 503') return 'La API responde, pero informa de que la base de datos no está disponible (HTTP 503)';
    if (error?.startsWith('HTTP')) return `La API respondió con un error (${error})`;
    return 'No hay respuesta del backend (túnel cerrado, API parada o CORS)';
  }

  // Parametros de la URL con los filtros actuales. Se usa igual para /api/eventos y /api/estadisticas.
  private query(extra: Record<string, string> = {}): string {
    const p = new URLSearchParams(extra);
    p.set('source', this.fSource);                                          // live = datos reales; replay = reproduccion aislada
    if (this.fStart) p.set('start', `${this.fStart}T00:00:00Z`);          // desde el inicio de ese dia UTC
    if (this.fEnd) {                                                        // "hasta" incluye el dia entero: end = dia siguiente (exclusivo)
      const d = new Date(`${this.fEnd}T00:00:00Z`); d.setUTCDate(d.getUTCDate() + 1);
      p.set('end', d.toISOString());
    }
    if (this.fMag !== '' && Number.isFinite(Number(this.fMag))) p.set('min_mag', this.fMag);
    if (this.fCountry) p.set('country_id', this.fCountry);
    return p.toString();
  }

  private async refresh(): Promise<void> {
    const config = this.config;
    if (!config || this.destroyed) return;
    const token = ++this.seq;
    const api = config.mode === 'api';
    const base = config.apiUrl.replace(/\/$/, '');
    const headers: Record<string, string> = {};
    if (api && config.skipNgrokWarning) headers['ngrok-skip-browser-warning'] = '1';
    // Las dos consultas salen a la vez (mapa/tabla y graficos) con exactamente los mismos filtros
    const [ev, st] = await Promise.all([
      this.tryJson<Events>(api ? `${base}/api/eventos?${this.query({ limit: '5000' })}` : 'datos/terremotos-demo.geojson', headers),
      api ? this.tryJson<Stats>(`${base}/api/estadisticas?${this.query()}`, headers) : Promise.resolve(null),
    ]);
    if (this.destroyed || token !== this.seq) return;

    // ---- Mapa, tabla y estado del sistema ----
    if (ev.data) {
      try {
        this.draw(ev.data, config);
        this.backendError.set(null);
        this.lastRead.set(new Date().toISOString());
      } catch { this.backendError.set('La respuesta de la API no tiene el formato esperado. Se conserva la última lectura.'); }
    } else {
      this.backendError.set(`${this.explain(ev.error)}. Se conserva la última lectura.`);
      this.status.set('No se pudo actualizar el mapa');
    }
    // ---- Graficos ----
    if (st === null) {
      this.stats.set(null); this.statsError.set('Las estadísticas solo están disponibles en modo API.');
    } else if (st.data) {
      this.stats.set(st.data); this.statsError.set(null);
    } else {
      this.statsError.set(`${this.explain(st.error)}. Se conservan los gráficos anteriores.`);
    }
    // La siguiente consulta se programa al terminar esta (nunca dos a la vez), cada 60 s como pide el enunciado
    if (!this.destroyed && api) this.timer = setTimeout(() => void this.refresh(), Math.max(30000, Number(config.refreshMs) || 60000));
  }

  private draw(data: Events, config: Config): void {
    if (data.type !== 'FeatureCollection' || !Array.isArray(data.features)) throw new Error('Se esperaba FeatureCollection');
    // Construir primero la nueva capa: si falla, conservar la anterior.
    const next = L.layerGroup(); let omitted = 0; let count = 0; const filas: Fila[] = [];
    for (const feature of data.features) {
      const p = feature.properties;
      const c = feature.geometry?.type === 'Point' ? feature.geometry.coordinates : [];
      const [lon, lat, depth] = c;
      if (!p || !Number.isFinite(lon) || !Number.isFinite(lat) || Math.abs(lon) > 180 || Math.abs(lat) > 90) { omitted++; continue; }
      const magnitude = typeof p.mag === 'number' && Number.isFinite(p.mag) ? p.mag : null;
      const depthOk = typeof depth === 'number' && Number.isFinite(depth);
      const color = !depthOk ? '#64748b' : depth < 70 ? '#e8a317' : depth < 300 ? '#e35b37' : '#7b3fb6';
      const pais = p.country_name ?? 'Sin país asignado';
      const popup = document.createElement('div');
      popup.textContent = `${p.place ?? 'Sin descripción'}\nMagnitud: ${magnitude ?? 'ND'}${p.magType ? ' (' + p.magType + ')' : ''}\nProfundidad: ${depthOk ? depth + ' km' : 'ND'}\nFecha UTC: ${this.date(p.time)}\nPaís: ${pais}\nID: ${feature.id ?? 'ND'}`;
      L.circleMarker([lat, lon], { radius: magnitude === null ? 4 : Math.max(3, Math.min(18, 3 + (magnitude + 1) * 2)), color, fillColor: color, fillOpacity: .75, weight: 1 }).bindPopup(popup).addTo(next);
      filas.push({ id: String(feature.id ?? 'ND'), fecha: this.date(p.time), mag: magnitude === null ? 'ND' : String(magnitude),
                   prof: depthOk ? `${depth} km` : 'ND', pais, lugar: p.place ?? 'Sin descripción' });
      count++;
    }
    this.events.clearLayers(); this.events.addLayer(next);
    this.count.set(count); this.omitted.set(omitted); this.rows.set(filas);
    this.generated.set(this.date(data.metadata?.generated));
    this.lastIngestion.set(this.date(data.metadata?.last_ingestion));
    // Estado del sistema: periodo consultado y avisos
    const f = data.metadata?.filters;
    const rp = data.metadata?.mode === 'replay';
    this.replay.set(rp);
    const rango = rp && data.metadata?.data_range ? ` · intervalo de los datos reproducidos: ${this.date(data.metadata.data_range.min)} → ${this.date(data.metadata.data_range.max)}` : '';
    this.periodText.set(f ? `${f.start ?? 'sin inicio'} → ${f.end ?? (rp ? 'sin fin' : 'ahora')} · magnitud mínima: ${f.min_mag ?? 'sin filtro'} · país: ${f.country_id ?? 'todos'}${rango}` : 'Datos de demostración');
    const m = data.metadata;
    this.truncatedMsg.set(m?.truncated ? `Hay ${m.total} eventos y solo se dibujan ${m.count}: acota el periodo o sube la magnitud mínima.` : null);
    const li = m?.last_ingestion;
    if (config.mode === 'demo' || rp) this.stale.set(null);   // en reproduccion no hay ingesta de USGS que vigilar
    else if (typeof li !== 'number') this.stale.set('La API no tiene ninguna ingesta correcta registrada: el productor puede no haber arrancado.');
    else if (Date.now() - li > STALE_MS) this.stale.set(`Datos desactualizados: la última ingesta correcta de USGS fue hace ${Math.round((Date.now() - li) / 60000)} min. El productor puede estar parado.`);
    else this.stale.set(null);
    this.status.set(config.mode === 'demo' ? 'Ejemplo estático: no representa actividad real' : 'API accesible · consulta completada');
  }

  // ---- Ayudas para los graficos de barras ----
  maxN(items: { n: number }[]): number { return items.reduce((m, x) => Math.max(m, x.n), 0); }
  pct(n: number, max: number): number { return max > 0 ? Math.round(n * 100 / max) : 0; }
  fmt(n: number | null | undefined): string { return typeof n === 'number' ? n.toLocaleString('es-ES') : 'ND'; }

  private date(value: unknown): string {
    if (typeof value !== 'number' || !Number.isFinite(value)) return 'ND';
    const d = new Date(value); return Number.isNaN(d.getTime()) ? 'ND' : d.toISOString();
  }
  ngOnDestroy(): void { this.destroyed = true; clearTimeout(this.timer); this.controller.abort(); }
}
