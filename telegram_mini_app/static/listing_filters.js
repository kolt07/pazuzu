/* listing_filters.js — уніфіковані фасетні відбори оголошень (FilterSpec). */
(function (global) {
  "use strict";

  function emptyFacets() {
    return {
      status: [],
      source: [],
      property_type: [],
      deal_type: [],
      price_currency: "uah",
      price_uah: { min: null, max: null },
      price_usd: { min: null, max: null },
      building_area_sqm: { min: null, max: null },
      land_area_sotky: { min: null, max: null },
      source_updated_at: { from: null, to: null },
      title_contains: "",
      description_contains: "",
      geo: {
        regions: [],
        settlements: [],
        city_districts: [],
        settlement_population: { min: null, max: null },
        settlement_area_sq_km: { min: null, max: null }
      }
    };
  }

  function emptySpec() {
    return { version: 1, group_type: "and", items: [] };
  }

  function clone(obj) {
    return JSON.parse(JSON.stringify(obj || null));
  }

  function numOrNull(v) {
    if (v === null || v === undefined || v === "") return null;
    var n = Number(v);
    return isNaN(n) ? null : n;
  }

  function facetsToFilterSpec(facets) {
    var f = facets || emptyFacets();
    var items = [];

    function addMulti(field, values) {
      var vals = Array.isArray(values) ? values : values ? [values] : [];
      vals = vals.filter(function (v) { return v != null && String(v).trim() !== ""; });
      if (!vals.length) return;
      if (vals.length === 1) {
        items.push({
          type: "element",
          field: field,
          operator: "eq",
          value: field === "source" ? String(vals[0]).toLowerCase() : vals[0]
        });
      } else {
        items.push({
          type: "element",
          field: field,
          operator: "in",
          value: vals.map(function (v) {
            return field === "source" ? String(v).toLowerCase() : v;
          })
        });
      }
    }

    addMulti("status", f.status);
    addMulti("source", f.source);
    addMulti("property_type", f.property_type);
    addMulti("deal_type", f.deal_type);

    var currency = (f.price_currency || "uah").toLowerCase() === "usd" ? "usd" : "uah";
    var priceField = currency === "usd" ? "price_usd" : "price_uah";
    var priceRng = f[priceField] || {};
    var pMin = numOrNull(priceRng.min);
    var pMax = numOrNull(priceRng.max);
    if (pMin != null) items.push({ type: "element", field: priceField, operator: "gte", value: pMin });
    if (pMax != null) items.push({ type: "element", field: priceField, operator: "lte", value: pMax });

    ["building_area_sqm", "land_area_sotky"].forEach(function (field) {
      var rng = f[field] || {};
      var mn = numOrNull(rng.min);
      var mx = numOrNull(rng.max);
      if (mn != null) items.push({ type: "element", field: field, operator: "gte", value: mn });
      if (mx != null) items.push({ type: "element", field: field, operator: "lte", value: mx });
    });

    var dates = f.source_updated_at || {};
    if (dates.from) items.push({ type: "element", field: "source_updated_at", operator: "gte", value: dates.from });
    if (dates.to) items.push({ type: "element", field: "source_updated_at", operator: "lte", value: dates.to });

    if ((f.title_contains || "").trim()) {
      items.push({ type: "element", field: "title", operator: "contains", value: f.title_contains.trim() });
    }
    if ((f.description_contains || "").trim()) {
      items.push({
        type: "element",
        field: "description",
        operator: "contains",
        value: f.description_contains.trim()
      });
    }

    var geo = f.geo || {};
    var regions = geo.regions || [];
    if (regions.length === 1) {
      var r = regions[0];
      var gi = { type: "geo", geo_type: "region", operator: "inside", value: r.name || r.value || "" };
      if (r.id || r.region_id) gi.region_id = String(r.id || r.region_id);
      items.push(gi);
    } else if (regions.length > 1) {
      items.push({
        type: "group",
        group_type: "or",
        items: regions.map(function (rd) {
          var g = { type: "geo", geo_type: "region", operator: "inside", value: rd.name || rd.value || "" };
          if (rd.id || rd.region_id) g.region_id = String(rd.id || rd.region_id);
          return g;
        })
      });
    }

    var settlements = geo.settlements || [];
    function settlementItem(s) {
      var g = {
        type: "geo",
        geo_type: "settlement",
        operator: "inside",
        value: s.name || s.value || ""
      };
      if (s.city_id || s.cityId) g.city_id = String(s.city_id || s.cityId);
      if (s.region_id || s.regionId) g.region_id = String(s.region_id || s.regionId);
      if (s.region || s.geoRegion) g.geoRegion = s.region || s.geoRegion;
      return g;
    }
    if (settlements.length === 1) {
      items.push(settlementItem(settlements[0]));
    } else if (settlements.length > 1) {
      items.push({
        type: "group",
        group_type: "or",
        items: settlements.map(settlementItem)
      });
    }

    (geo.city_districts || []).forEach(function (d) {
      var name = typeof d === "string" ? d : (d && d.name) || "";
      if (name.trim()) {
        items.push({ type: "geo", geo_type: "city_district", operator: "inside", value: name.trim() });
      }
    });

    var pop = geo.settlement_population || {};
    if (pop.min != null || pop.max != null) {
      var pg = { type: "geo", geo_type: "settlement_population", operator: "inside", value: "" };
      if (pop.min != null && pop.min !== "") pg.population_min = parseInt(pop.min, 10);
      if (pop.max != null && pop.max !== "") pg.population_max = parseInt(pop.max, 10);
      items.push(pg);
    }
    var area = geo.settlement_area_sq_km || {};
    if (area.min != null || area.max != null) {
      var ag = { type: "geo", geo_type: "settlement_area", operator: "inside", value: "" };
      if (area.min != null && area.min !== "") ag.area_min = parseFloat(area.min);
      if (area.max != null && area.max !== "") ag.area_max = parseFloat(area.max);
      items.push(ag);
    }

    return { version: 1, group_type: "and", items: items };
  }

  function filterSpecSummary(spec) {
    if (!spec || !(spec.items || []).length) return "";
    var parts = [];
    function walk(items) {
      (items || []).forEach(function (it) {
        if (!it) return;
        if (it.type === "element") {
          if (it.field === "source") parts.push(String(it.value).toUpperCase());
          else if (it.field === "property_type") parts.push(String(it.value));
          else if (it.field === "status") parts.push(String(it.value));
          else if (it.field === "deal_type") parts.push(it.value === "rent" ? "Оренда" : it.value === "sale" ? "Продаж" : String(it.value));
          else if (it.field === "price_uah" || it.field === "price_usd") {
            var unit = it.field === "price_usd" ? "$" : "грн";
            if (it.operator === "gte") parts.push("ціна ≥ " + it.value + " " + unit);
            else if (it.operator === "lte") parts.push("ціна ≤ " + it.value + " " + unit);
          } else if (it.field === "title" && it.operator === "contains") {
            parts.push("«" + it.value + "»");
          }
        } else if (it.type === "geo") {
          if (it.value) parts.push(String(it.value));
          else if (it.geo_type === "settlement_population") parts.push("населення НП");
          else if (it.geo_type === "settlement_area") parts.push("площа НП");
        } else if (it.type === "group") {
          walk(it.items);
        }
      });
    }
    walk(spec.items);
    return parts.filter(Boolean).join(", ");
  }

  function buildChips(spec, facets, isSimple) {
    var chips = [];
    if (!isSimple || !facets) {
      var n = (spec && spec.items && spec.items.length) || 0;
      if (n) chips.push({ key: "advanced", label: "Розширені відбори (" + n + ")", remove: "clear" });
      return chips;
    }
    (facets.source || []).forEach(function (s, i) {
      chips.push({ key: "source:" + i, label: String(s).toUpperCase(), remove: { facet: "source", index: i } });
    });
    (facets.property_type || []).forEach(function (s, i) {
      chips.push({ key: "pt:" + i, label: String(s), remove: { facet: "property_type", index: i } });
    });
    (facets.status || []).forEach(function (s, i) {
      chips.push({ key: "st:" + i, label: String(s), remove: { facet: "status", index: i } });
    });
    (facets.deal_type || []).forEach(function (s, i) {
      var lab = s === "rent" ? "Оренда" : s === "sale" ? "Продаж" : String(s);
      chips.push({ key: "dt:" + i, label: lab, remove: { facet: "deal_type", index: i } });
    });
    var price = facets.price_uah || {};
    var priceUsd = facets.price_usd || {};
    var cur = (facets.price_currency || "uah").toLowerCase();
    if (cur === "usd" && (priceUsd.min != null || priceUsd.max != null)) {
      var plu =
        priceUsd.min != null && priceUsd.max != null
          ? "$ " + priceUsd.min + "–" + priceUsd.max
          : priceUsd.min != null
            ? "$ ≥ " + priceUsd.min
            : "$ ≤ " + priceUsd.max;
      chips.push({ key: "price", label: plu, remove: { facet: "price_usd" } });
    } else if (price.min != null || price.max != null) {
      var pl =
        price.min != null && price.max != null
          ? "ціна " + price.min + "–" + price.max + " грн"
          : price.min != null
            ? "ціна ≥ " + price.min + " грн"
            : "ціна ≤ " + price.max + " грн";
      chips.push({ key: "price", label: pl, remove: { facet: "price_uah" } });
    }
    ((facets.geo && facets.geo.regions) || []).forEach(function (r, i) {
      chips.push({
        key: "reg:" + i,
        label: r.name || r.value || "область",
        remove: { facet: "geo.regions", index: i }
      });
    });
    ((facets.geo && facets.geo.settlements) || []).forEach(function (s, i) {
      chips.push({
        key: "set:" + i,
        label: s.name || s.value || "НП",
        remove: { facet: "geo.settlements", index: i }
      });
    });
    ((facets.geo && facets.geo.city_districts) || []).forEach(function (d, i) {
      chips.push({
        key: "dist:" + i,
        label: typeof d === "string" ? d : d.name || "район",
        remove: { facet: "geo.city_districts", index: i }
      });
    });
    if ((facets.title_contains || "").trim()) {
      chips.push({
        key: "title",
        label: "«" + facets.title_contains.trim() + "»",
        remove: { facet: "title_contains" }
      });
    }
    return chips;
  }

  function removeFromFacets(facets, remove) {
    var f = clone(facets) || emptyFacets();
    if (!remove || remove === "clear") return emptyFacets();
    if (remove.facet === "price_uah") {
      f.price_uah = { min: null, max: null };
      return f;
    }
    if (remove.facet === "price_usd") {
      f.price_usd = { min: null, max: null };
      return f;
    }
    if (remove.facet === "title_contains") {
      f.title_contains = "";
      return f;
    }
    if (remove.facet === "geo.regions") {
      f.geo.regions.splice(remove.index, 1);
      return f;
    }
    if (remove.facet === "geo.settlements") {
      f.geo.settlements.splice(remove.index, 1);
      return f;
    }
    if (remove.facet === "geo.city_districts") {
      f.geo.city_districts.splice(remove.index, 1);
      return f;
    }
    if (Array.isArray(f[remove.facet])) {
      f[remove.facet].splice(remove.index, 1);
    }
    return f;
  }

  var PROPERTY_TYPES = [
    "Земельна ділянка",
    "Будівля",
    "Приміщення",
    "Квартира",
    "Будинок",
    "Інше"
  ];

  /**
   * Стан відборів для однієї поверхні (search | map | report | research).
   */
  function createSurfaceState(initialSpec) {
    return {
      filterSpec: clone(initialSpec) || emptySpec(),
      facets: emptyFacets(),
      isSimple: true,
      draftFacets: emptyFacets()
    };
  }

  var surfaces = {
    search: createSurfaceState(),
    map: createSurfaceState(),
    report: createSurfaceState(),
    research: createSurfaceState()
  };

  var regionOptions = [];
  var activePanelSurface = null;
  var onApplyCallbacks = {};

  function getState(surface) {
    return surfaces[surface] || surfaces.search;
  }

  function setFilterSpec(surface, spec, opts) {
    var st = getState(surface);
    st.filterSpec = clone(spec) || emptySpec();
    opts = opts || {};
    if (opts.facets != null) {
      st.facets = clone(opts.facets);
      st.isSimple = opts.isSimple !== false;
    } else if (opts.skipFacetSync) {
      /* keep */
    } else {
      // optimistic: treat as simple if AND root; full sync via API when available
      st.isSimple = (st.filterSpec.group_type || "and") === "and";
      if (!(st.filterSpec.items || []).length) {
        st.facets = emptyFacets();
        st.isSimple = true;
      }
    }
    st.draftFacets = clone(st.facets);
    renderChips(surface);
    updateSummary(surface);
  }

  function getFilterSpec(surface) {
    return clone(getState(surface).filterSpec);
  }

  function syncFacetsFromServer(surface, apiFetch) {
    var st = getState(surface);
    if (!(st.filterSpec.items || []).length) {
      st.facets = emptyFacets();
      st.isSimple = true;
      st.draftFacets = emptyFacets();
      renderChips(surface);
      updateSummary(surface);
      return Promise.resolve(st);
    }
    return apiFetch("/api/search/filter-to-facets", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ filter: st.filterSpec })
    })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        st.isSimple = !!data.is_simple;
        st.facets = data.facets || emptyFacets();
        st.draftFacets = clone(st.facets);
        renderChips(surface);
        updateSummary(surface);
        return st;
      })
      .catch(function () {
        st.isSimple = false;
        renderChips(surface);
        updateSummary(surface);
        return st;
      });
  }

  function applyFacets(surface, facets) {
    var st = getState(surface);
    st.facets = clone(facets) || emptyFacets();
    st.draftFacets = clone(st.facets);
    st.filterSpec = facetsToFilterSpec(st.facets);
    st.isSimple = true;
    renderChips(surface);
    updateSummary(surface);
    return st.filterSpec;
  }

  function clearFilters(surface) {
    setFilterSpec(surface, emptySpec(), { facets: emptyFacets(), isSimple: true });
  }

  function updateSummary(surface) {
    var st = getState(surface);
    var text = filterSpecSummary(st.filterSpec);
    var elId =
      surface === "map"
        ? "map-filter-summary-text"
        : surface === "report"
          ? "constructor-filter-summary-text"
          : surface === "research"
            ? "research-filter-summary-text"
            : "search-filter-summary-text";
    var el = document.getElementById(elId);
    if (el) {
      el.textContent = text || "Відбори не задано";
    }
  }

  function renderChips(surface) {
    var containerId =
      surface === "map"
        ? "map-filter-chips"
        : surface === "report"
          ? "constructor-filter-chips"
          : surface === "research"
            ? "research-filter-chips"
            : "search-filter-chips";
    var el = document.getElementById(containerId);
    if (!el) return;
    var st = getState(surface);
    var chips = buildChips(st.filterSpec, st.facets, st.isSimple);
    if (!chips.length) {
      el.innerHTML = "";
      el.classList.add("hidden");
      return;
    }
    el.classList.remove("hidden");
    el.innerHTML = chips
      .map(function (c) {
        return (
          '<button type="button" class="filter-chip" data-chip-key="' +
          encodeURIComponent(c.key) +
          '"><span class="filter-chip-label">' +
          escapeHtml(c.label) +
          '</span><span class="filter-chip-x" aria-label="Зняти">×</span></button>'
        );
      })
      .join("");
    el.querySelectorAll(".filter-chip").forEach(function (btn, idx) {
      btn.addEventListener("click", function () {
        var chip = chips[idx];
        if (!chip) return;
        if (!st.isSimple || chip.remove === "clear") {
          clearFilters(surface);
        } else {
          applyFacets(surface, removeFromFacets(st.facets, chip.remove));
        }
        if (onApplyCallbacks[surface]) onApplyCallbacks[surface]();
      });
    });
  }

  function escapeHtml(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function toggleInArray(arr, value, checked) {
    var i = arr.indexOf(value);
    if (checked && i < 0) arr.push(value);
    if (!checked && i >= 0) arr.splice(i, 1);
  }

  function renderFacetPanel(surface, opts) {
    opts = opts || {};
    var panel = document.getElementById("facet-filters-panel");
    var body = document.getElementById("facet-filters-body");
    if (!panel || !body) return;
    activePanelSurface = surface;
    var st = getState(surface);
    var f;
    if (opts.keepDraft && st.draftFacets) {
      f = clone(st.draftFacets);
    } else {
      f = clone(st.isSimple ? st.facets : emptyFacets()) || emptyFacets();
      if (!f.price_currency) f.price_currency = "uah";
      if (!f.price_usd) f.price_usd = { min: null, max: null };
    }
    st.draftFacets = f;

    var currency = (f.price_currency || "uah").toLowerCase() === "usd" ? "usd" : "uah";
    var priceRng = currency === "usd" ? f.price_usd || {} : f.price_uah || {};

    var advancedNote = !st.isSimple
      ? '<p class="facet-advanced-note">Зараз активні розширені відбори. Зміна фасетів замінить їх.</p>'
      : "";

    body.innerHTML =
      advancedNote +
      section(
        "Локація",
        '<p class="facet-hint">Область = усі населені пункти області. Місто/село — лише обраний НП (Львів ≠ Львівська область).</p>' +
          selectedRegionChipsHtml(f) +
          regionCheckboxes(f) +
          settlementPickerHtml(f) +
          rangeRow("Населення НП, осіб", "pop", f.geo.settlement_population) +
          rangeRow("Площа НП, км²", "area", f.geo.settlement_area_sq_km)
      ) +
      section(
        "Ціна",
        currencyToggleHtml(currency) +
          rangeRow(currency === "usd" ? "Ціна, USD" : "Ціна, грн", "price", priceRng)
      ) +
      section(
        "Площа",
        rangeRow("Нерухомість, м²", "building", f.building_area_sqm) +
          rangeRow("Земля, сотки", "land", f.land_area_sotky)
      ) +
      section(
        "Джерело та тип",
        checkboxGroup("source", [
          { v: "olx", l: "OLX" },
          { v: "prozorro", l: "ProZorro" }
        ], f.source) +
          checkboxGroup(
            "property_type",
            PROPERTY_TYPES.map(function (t) { return { v: t, l: t }; }),
            f.property_type
          ) +
          checkboxGroup(
            "status",
            [
              { v: "активне", l: "Активне" },
              { v: "неактивне", l: "Неактивне" }
            ],
            f.status
          ) +
          (surface === "research"
            ? ""
            : checkboxGroup(
                "deal_type",
                [
                  { v: "sale", l: "Продаж" },
                  { v: "rent", l: "Оренда" }
                ],
                f.deal_type
              ))
      ) +
      section(
        "Текст у оголошенні",
        '<div class="facet-field"><label>Заголовок містить</label>' +
          '<input type="text" class="filter-input" id="facet-title" value="' +
          escapeHtml(f.title_contains || "") +
          '" /></div>' +
          '<div class="facet-field"><label>Опис містить</label>' +
          '<input type="text" class="filter-input" id="facet-description" value="' +
          escapeHtml(f.description_contains || "") +
          '" /></div>'
      );

    bindFacetPanelEvents(surface);
    panel.classList.remove("hidden");
    document.body.classList.add("facet-panel-open");
  }

  function currencyToggleHtml(currency) {
    return (
      '<div class="facet-currency-toggle" role="group" aria-label="Валюта ціни">' +
      '<button type="button" class="facet-currency-btn' +
      (currency === "uah" ? " is-active" : "") +
      '" data-currency="uah">грн</button>' +
      '<button type="button" class="facet-currency-btn' +
      (currency === "usd" ? " is-active" : "") +
      '" data-currency="usd">$</button>' +
      "</div>"
    );
  }

  function selectedRegionChipsHtml(f) {
    var list = (f.geo && f.geo.regions) || [];
    if (!list.length) return '<div id="facet-region-chips" class="filter-chips facet-location-chips"></div>';
    return (
      '<div id="facet-region-chips" class="filter-chips facet-location-chips">' +
      list
        .map(function (r, i) {
          return (
            '<span class="filter-chip filter-chip-static">' +
            escapeHtml(r.name || "") +
            ' <button type="button" class="filter-chip-x facet-remove-region" data-idx="' +
            i +
            '" aria-label="Прибрати">×</button></span>'
          );
        })
        .join("") +
      "</div>"
    );
  }

  function section(title, inner) {
    return (
      '<details class="facet-section" open><summary class="facet-section-title">' +
      escapeHtml(title) +
      '</summary><div class="facet-section-body">' +
      inner +
      "</div></details>"
    );
  }

  function checkboxGroup(name, options, selected) {
    var sel = selected || [];
    return (
      '<div class="facet-checkboxes" data-facet="' +
      name +
      '">' +
      options
        .map(function (o) {
          var checked = sel.indexOf(o.v) >= 0 ? " checked" : "";
          return (
            '<label class="facet-check"><input type="checkbox" value="' +
            escapeHtml(o.v) +
            '"' +
            checked +
            " /> " +
            escapeHtml(o.l) +
            "</label>"
          );
        })
        .join("") +
      "</div>"
    );
  }

  function rangeRow(label, key, rng) {
    rng = rng || {};
    return (
      '<div class="facet-range" data-range="' +
      key +
      '"><label>' +
      escapeHtml(label) +
      '</label><div class="facet-range-inputs">' +
      '<input type="number" class="filter-input facet-min" placeholder="від" inputmode="decimal" value="' +
      (rng.min != null ? rng.min : "") +
      '" />' +
      '<input type="number" class="filter-input facet-max" placeholder="до" inputmode="decimal" value="' +
      (rng.max != null ? rng.max : "") +
      '" /></div></div>'
    );
  }

  function regionCheckboxes(f) {
    var selectedIds = ((f.geo && f.geo.regions) || []).map(function (r) {
      return String(r.id || r.region_id || r.name || "");
    });
    if (!regionOptions.length) {
      return '<p class="facet-hint">Завантаження областей…</p><div id="facet-regions" class="facet-checkboxes facet-regions"></div>';
    }
    return (
      '<div class="facet-field"><label>Області <span class="facet-hint-inline">уся область, не лише центр</span></label>' +
      '<input type="search" id="facet-region-filter" class="filter-input" placeholder="Швидкий пошук області…" autocomplete="off" />' +
      '<div class="facet-checkboxes facet-regions" id="facet-regions">' +
      regionOptions
        .map(function (r) {
          var id = String(r.id || r.name);
          var checked = selectedIds.indexOf(id) >= 0 || selectedIds.indexOf(r.name) >= 0 ? " checked" : "";
          return (
            '<label class="facet-check" data-region-label="' +
            escapeHtml((r.name || "").toLowerCase()) +
            '"><input type="checkbox" data-region-id="' +
            escapeHtml(r.id || "") +
            '" data-region-name="' +
            escapeHtml(r.name || "") +
            '" value="' +
            escapeHtml(id) +
            '"' +
            checked +
            " /> " +
            escapeHtml(r.name) +
            "</label>"
          );
        })
        .join("") +
      "</div></div>"
    );
  }

  function settlementPickerHtml(f) {
    var list = (f.geo && f.geo.settlements) || [];
    return (
      '<div class="facet-field facet-settlement-field">' +
      "<label>Населені пункти <span class=\"facet-hint-inline\">точний НП, можна кілька</span></label>" +
      '<div id="facet-settlement-chips" class="filter-chips facet-location-chips">' +
      renderSettlementChipsHtml(list) +
      "</div>" +
      '<input type="search" id="facet-settlement-search" class="filter-input" placeholder="Почніть вводити назву НП…" autocomplete="off" />' +
      '<div id="facet-settlement-results" class="facet-settlement-results hidden"></div>' +
      "</div>"
    );
  }

  function renderSettlementChipsHtml(list) {
    if (!list || !list.length) {
      return '<span class="facet-chips-empty">Ще не обрано</span>';
    }
    return list
      .map(function (s, i) {
        return (
          '<span class="filter-chip filter-chip-static" data-settlement-idx="' +
          i +
          '">' +
          escapeHtml(s.name || "") +
          (s.region ? ' <span class="muted">(' + escapeHtml(s.region) + ")</span>" : "") +
          ' <button type="button" class="filter-chip-x facet-remove-settlement" data-idx="' +
          i +
          '" aria-label="Прибрати">×</button></span>'
        );
      })
      .join("");
  }

  function refreshSettlementChips(surface) {
    var st = getState(surface);
    var el = document.getElementById("facet-settlement-chips");
    if (!el) return;
    el.innerHTML = renderSettlementChipsHtml((st.draftFacets.geo && st.draftFacets.geo.settlements) || []);
    el.querySelectorAll(".facet-remove-settlement").forEach(function (btn) {
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        e.stopPropagation();
        syncDraftFromDom(surface);
        var idx = parseInt(btn.getAttribute("data-idx"), 10);
        st.draftFacets.geo.settlements.splice(idx, 1);
        refreshSettlementChips(surface);
      });
    });
  }

  function syncDraftFromDom(surface) {
    var st = getState(surface);
    var f = st.draftFacets || emptyFacets();
    if (!f.geo) f.geo = emptyFacets().geo;
    if (!f.price_uah) f.price_uah = { min: null, max: null };
    if (!f.price_usd) f.price_usd = { min: null, max: null };

    document.querySelectorAll("#facet-filters-body .facet-checkboxes[data-facet]").forEach(function (box) {
      var name = box.getAttribute("data-facet");
      f[name] = [];
      box.querySelectorAll('input[type="checkbox"]:checked').forEach(function (cb) {
        f[name].push(cb.value);
      });
    });

    f.geo.regions = [];
    document.querySelectorAll("#facet-regions input[type=checkbox]:checked").forEach(function (cb) {
      f.geo.regions.push({
        id: cb.getAttribute("data-region-id") || null,
        name: cb.getAttribute("data-region-name") || cb.value
      });
    });

    var currencyBtn = document.querySelector(".facet-currency-btn.is-active");
    f.price_currency = currencyBtn ? currencyBtn.getAttribute("data-currency") || "uah" : f.price_currency || "uah";

    var price = document.querySelector('.facet-range[data-range="price"]');
    if (price) {
      var target = f.price_currency === "usd" ? f.price_usd : f.price_uah;
      target.min = numOrNull(price.querySelector(".facet-min").value);
      target.max = numOrNull(price.querySelector(".facet-max").value);
    }
    var building = document.querySelector('.facet-range[data-range="building"]');
    if (building) {
      f.building_area_sqm.min = numOrNull(building.querySelector(".facet-min").value);
      f.building_area_sqm.max = numOrNull(building.querySelector(".facet-max").value);
    }
    var land = document.querySelector('.facet-range[data-range="land"]');
    if (land) {
      f.land_area_sotky.min = numOrNull(land.querySelector(".facet-min").value);
      f.land_area_sotky.max = numOrNull(land.querySelector(".facet-max").value);
    }
    var pop = document.querySelector('.facet-range[data-range="pop"]');
    if (pop) {
      f.geo.settlement_population.min = numOrNull(pop.querySelector(".facet-min").value);
      f.geo.settlement_population.max = numOrNull(pop.querySelector(".facet-max").value);
    }
    var area = document.querySelector('.facet-range[data-range="area"]');
    if (area) {
      f.geo.settlement_area_sq_km.min = numOrNull(area.querySelector(".facet-min").value);
      f.geo.settlement_area_sq_km.max = numOrNull(area.querySelector(".facet-max").value);
    }

    var titleEl = document.getElementById("facet-title");
    var descEl = document.getElementById("facet-description");
    f.title_contains = titleEl ? titleEl.value : "";
    f.description_contains = descEl ? descEl.value : "";

    st.draftFacets = f;
    return f;
  }

  function readDraftFromPanel(surface) {
    return syncDraftFromDom(surface);
  }

  function addSettlementToDraft(surface, entry) {
    var st = getState(surface);
    syncDraftFromDom(surface);
    st.draftFacets.geo.settlements = st.draftFacets.geo.settlements || [];
    var idx = -1;
    for (var i = 0; i < st.draftFacets.geo.settlements.length; i++) {
      var s = st.draftFacets.geo.settlements[i];
      if (entry.city_id && s.city_id && String(s.city_id) === String(entry.city_id)) {
        idx = i;
        break;
      }
      if (
        (s.name || "").toLowerCase() === (entry.name || "").toLowerCase() &&
        (s.region || "").toLowerCase() === (entry.region || "").toLowerCase()
      ) {
        idx = i;
        break;
      }
    }
    if (idx >= 0) {
      st.draftFacets.geo.settlements.splice(idx, 1);
    } else {
      st.draftFacets.geo.settlements.push(entry);
    }
    refreshSettlementChips(surface);
  }

  function bindFacetPanelEvents(surface) {
    var st = getState(surface);

    document.querySelectorAll(".facet-remove-region").forEach(function (btn) {
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        e.stopPropagation();
        syncDraftFromDom(surface);
        var idx = parseInt(btn.getAttribute("data-idx"), 10);
        st.draftFacets.geo.regions.splice(idx, 1);
        renderFacetPanel(surface, { keepDraft: true });
      });
    });

    refreshSettlementChips(surface);

    document.querySelectorAll(".facet-currency-btn").forEach(function (btn) {
      btn.addEventListener("click", function () {
        syncDraftFromDom(surface);
        st.draftFacets.price_currency = btn.getAttribute("data-currency") || "uah";
        renderFacetPanel(surface, { keepDraft: true });
      });
    });

    var regionFilter = document.getElementById("facet-region-filter");
    if (regionFilter) {
      regionFilter.addEventListener("input", function () {
        var q = regionFilter.value.trim().toLowerCase();
        document.querySelectorAll("#facet-regions .facet-check").forEach(function (lab) {
          var label = lab.getAttribute("data-region-label") || "";
          lab.style.display = !q || label.indexOf(q) !== -1 ? "" : "none";
        });
      });
    }

    // Keep region chips in sync when checkboxes change
    document.querySelectorAll("#facet-regions input[type=checkbox]").forEach(function (cb) {
      cb.addEventListener("change", function () {
        syncDraftFromDom(surface);
        var chips = document.getElementById("facet-region-chips");
        if (chips) {
          var list = st.draftFacets.geo.regions || [];
          chips.innerHTML = list
            .map(function (r, i) {
              return (
                '<span class="filter-chip filter-chip-static">' +
                escapeHtml(r.name || "") +
                ' <button type="button" class="filter-chip-x facet-remove-region" data-idx="' +
                i +
                '">×</button></span>'
              );
            })
            .join("");
          chips.querySelectorAll(".facet-remove-region").forEach(function (btn) {
            btn.addEventListener("click", function (e) {
              e.preventDefault();
              syncDraftFromDom(surface);
              st.draftFacets.geo.regions.splice(parseInt(btn.getAttribute("data-idx"), 10), 1);
              renderFacetPanel(surface, { keepDraft: true });
            });
          });
        }
      });
    });

    var searchInput = document.getElementById("facet-settlement-search");
    var resultsEl = document.getElementById("facet-settlement-results");
    var timer = null;
    if (searchInput && resultsEl && global.ListingFilters._apiFetch) {
      searchInput.addEventListener("input", function () {
        clearTimeout(timer);
        var q = searchInput.value.trim();
        if (q.length < 2) {
          resultsEl.classList.add("hidden");
          resultsEl.innerHTML = "";
          return;
        }
        timer = setTimeout(function () {
          syncDraftFromDom(surface);
          var regs = (st.draftFacets.geo && st.draftFacets.geo.regions) || [];
          var regionIds = regs
            .map(function (r) { return r.id || r.region_id; })
            .filter(Boolean);
          var url =
            "/api/search/unified/filters/settlements/search?q=" +
            encodeURIComponent(q);
          if (regionIds.length === 1) {
            url += "&region_id=" + encodeURIComponent(regionIds[0]);
          } else if (regionIds.length > 1) {
            url += "&region_ids=" + encodeURIComponent(regionIds.join(","));
          }
          global.ListingFilters._apiFetch(url)
            .then(function (r) { return r.json(); })
            .then(function (data) {
              var items = data.options || data.settlements || data.items || data.results || [];
              if (!items.length) {
                resultsEl.innerHTML = '<div class="facet-settlement-empty">Нічого не знайдено</div>';
                resultsEl.classList.remove("hidden");
                return;
              }
              resultsEl.innerHTML = items
                .slice(0, 12)
                .map(function (it) {
                  var name = it.name || it.label || it.settlement || "";
                  var region = it.region || it.region_name || "";
                  var cityId = it.city_id || it.id || it._id || "";
                  var regionId2 = it.region_id || "";
                  var already = (st.draftFacets.geo.settlements || []).some(function (s) {
                    return cityId && s.city_id && String(s.city_id) === String(cityId);
                  });
                  return (
                    '<button type="button" class="facet-settlement-option' +
                    (already ? " is-selected" : "") +
                    '" data-name="' +
                    escapeHtml(name) +
                    '" data-region="' +
                    escapeHtml(region) +
                    '" data-city-id="' +
                    escapeHtml(cityId) +
                    '" data-region-id="' +
                    escapeHtml(regionId2) +
                    '">' +
                    escapeHtml(name) +
                    (region ? ' <span class="muted">' + escapeHtml(region) + "</span>" : "") +
                    (already ? ' <span class="facet-option-badge">обрано</span>' : "") +
                    "</button>"
                  );
                })
                .join("");
              resultsEl.classList.remove("hidden");
              resultsEl.querySelectorAll(".facet-settlement-option").forEach(function (btn) {
                btn.addEventListener("click", function () {
                  addSettlementToDraft(surface, {
                    name: btn.getAttribute("data-name"),
                    region: btn.getAttribute("data-region"),
                    city_id: btn.getAttribute("data-city-id"),
                    region_id: btn.getAttribute("data-region-id") || null
                  });
                  // Не очищаємо поле одразу — можна додати ще; підсвічуємо «обрано»
                  var wasSelected = btn.classList.contains("is-selected");
                  if (wasSelected) {
                    btn.classList.remove("is-selected");
                    var badge = btn.querySelector(".facet-option-badge");
                    if (badge) badge.remove();
                  } else {
                    btn.classList.add("is-selected");
                    if (!btn.querySelector(".facet-option-badge")) {
                      btn.insertAdjacentHTML(
                        "beforeend",
                        ' <span class="facet-option-badge">обрано</span>'
                      );
                    }
                  }
                  searchInput.focus();
                });
              });
            })
            .catch(function () {
              resultsEl.classList.add("hidden");
            });
        }, 280);
      });
    }
  }

  function closeFacetPanel() {
    var panel = document.getElementById("facet-filters-panel");
    if (panel) panel.classList.add("hidden");
    document.body.classList.remove("facet-panel-open");
  }

  function getActiveSurface() {
    return activePanelSurface || "search";
  }

  function applyPanel(surface) {
    var s = surface || activePanelSurface || "search";
    var facets = readDraftFromPanel(s);
    var spec = applyFacets(s, facets);
    closeFacetPanel();
    activePanelSurface = null;
    if (onApplyCallbacks[s]) onApplyCallbacks[s]();
    return spec;
  }

  function loadRegions(apiFetch) {
    return apiFetch("/api/search/unified/filters/regions")
      .then(function (r) { return r.json(); })
      .then(function (data) {
        var regs = data.regions || [];
        regionOptions = regs
          .map(function (r) {
            if (typeof r === "string") return { id: null, name: String(r).trim() };
            return { id: r.id || null, name: String(r.name || "").trim() };
          })
          .filter(function (r) {
            return r.name && r.name !== "null" && !/^\d+$/.test(r.name);
          });
        return regionOptions;
      })
      .catch(function () {
        regionOptions = [];
        return regionOptions;
      });
  }

  function onApply(surface, cb) {
    onApplyCallbacks[surface] = cb;
  }

  function openAdvanced(surface, openFilterBuilderFn) {
    activePanelSurface = surface;
    if (typeof openFilterBuilderFn === "function") {
      openFilterBuilderFn(surface);
    }
  }

  function applyTreeRoot(surface, root) {
    var spec = {
      version: 1,
      group_type: (root && root.group_type) || "and",
      items: (root && root.items) || []
    };
    setFilterSpec(surface, spec, { isSimple: false, facets: emptyFacets() });
    if (onApplyCallbacks[surface]) onApplyCallbacks[surface]();
    return spec;
  }

  global.ListingFilters = {
    emptySpec: emptySpec,
    emptyFacets: emptyFacets,
    facetsToFilterSpec: facetsToFilterSpec,
    filterSpecSummary: filterSpecSummary,
    getFilterSpec: getFilterSpec,
    setFilterSpec: setFilterSpec,
    applyFacets: applyFacets,
    clearFilters: clearFilters,
    renderChips: renderChips,
    updateSummary: updateSummary,
    openPanel: function (surface) {
      var self = this;
      if (!regionOptions.length && self._apiFetch) {
        loadRegions(self._apiFetch).then(function () {
          renderFacetPanel(surface);
        });
      } else {
        renderFacetPanel(surface);
      }
    },
    closePanel: closeFacetPanel,
    applyPanel: applyPanel,
    getActiveSurface: getActiveSurface,
    loadRegions: loadRegions,
    onApply: onApply,
    syncFacetsFromServer: syncFacetsFromServer,
    openAdvanced: openAdvanced,
    applyTreeRoot: applyTreeRoot,
    getState: getState,
    _apiFetch: null
  };
})(typeof window !== "undefined" ? window : globalThis);
