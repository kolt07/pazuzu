(function () {
  "use strict";

  var Tg = window.Telegram && window.Telegram.WebApp;
  var initData = "";
  
  // Функція для оновлення initData
  function updateInitData() {
    if (Tg && Tg.initData) {
      initData = Tg.initData;
    } else {
      // Fallback: спробуємо отримати з URL параметрів (для десктопного клієнта)
      var urlParams = new URLSearchParams(window.location.search);
      var tgInitData = urlParams.get("tgWebAppData");
      if (tgInitData) {
        initData = decodeURIComponent(tgInitData);
      }
    }
  }
  
  // Оновлюємо initData при завантаженні
  updateInitData();
  
  // Якщо Telegram WebApp API доступний, чекаємо на подію ready
  if (Tg) {
    if (Tg.ready) {
      Tg.ready();
    }
    if (Tg.expand) {
      Tg.expand();
    }
    // Для веб/десктоп — спроба запросити fullscreen (більше workspace)
    var platform = (Tg.platform || "").toLowerCase();
    var isDesktop = platform === "web" || platform === "tdesktop" || platform === "macos" || platform === "windows" || platform === "weba" || platform === "webk";
    if (isDesktop && Tg.requestFullscreen) {
      setTimeout(function() {
        try {
          Tg.requestFullscreen();
        } catch (e) {
          console.log("requestFullscreen skipped:", e);
        }
      }, 300);
    }
    if (Tg.onEvent) {
      Tg.onEvent("viewportChanged", function() {
        updateInitData();
      });
    }
    setTimeout(function() {
      updateInitData();
    }, 100);
    setTimeout(function() {
      updateInitData();
    }, 500);
  }

  function apiHeaders() {
    var h = { "Content-Type": "application/json" };
    updateInitData(); // Оновлюємо перед кожним запитом
    if (initData) h["X-Telegram-Init-Data"] = initData;
    return h;
  }

  var currentScreen = "screen-search";
  var currentUser = null;

  var CHAT_STORAGE_KEY = "pazuzu_chat_sessions";
  var THEME_STORAGE_KEY = "pazuzu_theme";
  var SIDEBAR_BREAKPOINT = 768;

  function getStoredTheme() {
    try {
      return localStorage.getItem(THEME_STORAGE_KEY);
    } catch (e) {
      return null;
    }
  }

  function applyTheme(theme) {
    var root = document.documentElement;
    if (theme === "dark") {
      root.setAttribute("data-theme", "dark");
    } else {
      root.removeAttribute("data-theme");
    }
  }

  function initTheme() {
    var stored = getStoredTheme();
    if (stored === "dark" || stored === "light") {
      applyTheme(stored);
      return;
    }
    var prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
    if (Tg && Tg.colorScheme === "dark") {
      applyTheme("dark");
    } else if (prefersDark) {
      applyTheme("dark");
    } else {
      applyTheme("light");
    }
  }

  initTheme();

  var chatSessions = [];
  var currentChatId = null;

  /** Контекст поточного оголошення/аукціону для переходу в чат з AI */
  var currentDetailContext = null;

  var sidebarCollapsed = false;
  var sidebarOverlayOpen = false;

  function isMobileOrNarrow() {
    var platform = Tg && (Tg.platform || "").toLowerCase();
    var isMobile = platform === "android" || platform === "ios";
    var isNarrow = typeof window !== "undefined" && window.innerWidth < SIDEBAR_BREAKPOINT;
    return isMobile || isNarrow;
  }

  function updateSidebarState() {
    var sidebar = document.getElementById("chat-sidebar");
    var toggleBtn = document.getElementById("sidebar-toggle");
    var backdrop = document.getElementById("sidebar-backdrop");
    if (!sidebar || !toggleBtn) return;

    if (currentScreen !== "screen-home") {
      sidebar.classList.add("hidden");
      toggleBtn.classList.add("hidden");
      if (backdrop) backdrop.classList.remove("visible");
      sidebarOverlayOpen = false;
      return;
    }

    sidebar.classList.remove("hidden");
    if (sidebarCollapsed) {
      sidebar.classList.add("collapsed");
      if (!sidebarOverlayOpen) sidebar.classList.remove("sidebar-overlay-open");
      toggleBtn.classList.remove("hidden");
      toggleBtn.textContent = "☰";
      toggleBtn.title = "Відкрити історію чатів";
    } else {
      sidebar.classList.remove("collapsed");
      sidebar.classList.remove("sidebar-overlay-open");
      toggleBtn.classList.add("hidden");
      if (backdrop) backdrop.classList.remove("visible");
      sidebarOverlayOpen = false;
    }
  }

  function openSidebarOverlay() {
    var sidebar = document.getElementById("chat-sidebar");
    var backdrop = document.getElementById("sidebar-backdrop");
    if (sidebar && backdrop) {
      sidebar.classList.add("sidebar-overlay-open");
      backdrop.classList.add("visible");
      sidebarOverlayOpen = true;
    }
  }

  function closeSidebarOverlay() {
    var sidebar = document.getElementById("chat-sidebar");
    var backdrop = document.getElementById("sidebar-backdrop");
    if (sidebar && backdrop) {
      sidebar.classList.remove("sidebar-overlay-open");
      backdrop.classList.remove("visible");
      sidebarOverlayOpen = false;
    }
  }

  function toggleSidebar() {
    if (sidebarCollapsed) {
      if (sidebarOverlayOpen) {
        closeSidebarOverlay();
      } else {
        openSidebarOverlay();
      }
    } else {
      sidebarCollapsed = true;
      updateSidebarState();
    }
  }

  function initSidebarCollapse() {
    sidebarCollapsed = isMobileOrNarrow();
    updateSidebarState();

    var toggleBtn = document.getElementById("sidebar-toggle");
    if (toggleBtn) {
      toggleBtn.addEventListener("click", toggleSidebar);
    }

    var backdrop = document.getElementById("sidebar-backdrop");
    if (backdrop) {
      backdrop.addEventListener("click", function () {
        closeSidebarOverlay();
      });
    }

    var collapseBtn = document.getElementById("sidebar-collapse-btn");
    if (collapseBtn) {
      collapseBtn.addEventListener("click", function () {
        sidebarCollapsed = true;
        closeSidebarOverlay();
        updateSidebarState();
      });
    }

    if (typeof window !== "undefined") {
      window.addEventListener("resize", function () {
        var shouldCollapse = isMobileOrNarrow();
        if (shouldCollapse && !sidebarCollapsed) {
          sidebarCollapsed = true;
          closeSidebarOverlay();
        } else if (!shouldCollapse && sidebarCollapsed) {
          sidebarCollapsed = false;
          closeSidebarOverlay();
        }
        updateSidebarState();
      });
    }
  }

  function loadChatSessions() {
    try {
      var raw = localStorage.getItem(CHAT_STORAGE_KEY);
      if (raw) {
        var parsed = JSON.parse(raw);
        chatSessions = Array.isArray(parsed) ? parsed : [];
      } else {
        chatSessions = [];
      }
    } catch (e) {
      console.warn("Failed to load chat sessions:", e);
      chatSessions = [];
    }
  }

  function saveChatSessions() {
    try {
      localStorage.setItem(CHAT_STORAGE_KEY, JSON.stringify(chatSessions));
    } catch (e) {
      console.warn("Failed to save chat sessions:", e);
    }
  }

  function genChatId() {
    return "chat_" + Date.now() + "_" + Math.random().toString(36).slice(2, 9);
  }

  function createNewChat() {
    var id = genChatId();
    chatSessions.unshift({
      id: id,
      title: "Новий чат",
      messages: [],
      updatedAt: Date.now()
    });
    saveChatSessions();
    currentChatId = id;
    renderChatHistoryList();
    renderChatMessages();
    var input = document.getElementById("chat-input");
    if (input) {
      input.placeholder = "Ваш запит...";
      input.focus();
    }
  }

  /**
   * Відкриває новий чат з контекстом оголошення/аукціону.
   * Контекст (посилання + короткий опис) передається окремо, не в тексті запиту.
   * @param {Object} context - { page_url: string, summary: string }
   */
  function openChatWithListingContext(context) {
    createNewChat();
    var chat = getCurrentChat();
    if (chat && context) {
      chat.listingContext = {
        page_url: context.page_url || "",
        summary: context.summary || "",
        detail_source: context.detail_source || "",
        detail_id: context.detail_id || "",
      };
      saveChatSessions();
      renderChatListingPinned();
    }
    show("screen-home");
    var input = document.getElementById("chat-input");
    if (input) {
      input.value = "Проаналізуй це оголошення (див. посилання вище). ";
      input.placeholder = "Наприклад: аналітика цін у районі, оцінка розташування...";
      input.focus();
      input.scrollIntoView({ behavior: "smooth" });
    }
  }

  /**
   * Будує короткий контекст оголошення (без заголовка — він на посиланні).
   * @returns {{ page_url: string, summary: string, detail_source: string, detail_id: string }}
   */
  function buildDetailContext(data, type) {
    var parts = [];
    var pageUrl = "";
    var detailSource = type || "";
    var detailId = "";
    if (type === "prozorro") {
      var aid = data.auction_id;
      detailId = aid || "";
      pageUrl = aid ? "https://prozorro.sale/auction/" + aid : "";
      var amt = getNestedValue(data, "auction_data.value.amount");
      if (amt != null) parts.push("ціна " + formatPrice(amt) + " ₴");
      var refs = (data.auction_data && data.auction_data.address_refs) || [];
      if (refs.length && refs[0]) {
        var locParts = [];
        if (refs[0].city && refs[0].city.name) locParts.push(refs[0].city.name);
        if (refs[0].region && refs[0].region.name) locParts.push(refs[0].region.name);
        if (locParts.length) parts.push(locParts.join(", "));
      }
      if (!parts.length && data.auction_data && data.auction_data.items && data.auction_data.items[0]) {
        var addr = data.auction_data.items[0].address;
        if (addr) {
          var locParts = [];
          if (addr.locality && (addr.locality.uk_UA || addr.locality.en_US)) locParts.push(addr.locality.uk_UA || addr.locality.en_US || "");
          if (addr.region && (addr.region.uk_UA || addr.region.en_US)) locParts.push(addr.region.uk_UA || addr.region.en_US || "");
          if (locParts.length) parts.push(locParts.filter(Boolean).join(", "));
        }
      }
    } else if (type === "olx") {
      pageUrl = data.url || "";
      detailId = data.url || data.id || data._id || "";
      var pv = getNestedValue(data, "search_data.price_value");
      if (pv != null) parts.push("ціна " + formatPrice(pv) + " ₴");
      var loc = getNestedValue(data, "search_data.location");
      var location = typeof loc === "string" ? loc : (loc && loc.city ? loc.city + (loc.region ? ", " + loc.region : "") : "");
      if (location) parts.push(location);
    }
    parts.push(type === "olx" ? "OLX" : "ProZorro");
    return { page_url: pageUrl, summary: parts.join(", "), detail_source: detailSource, detail_id: detailId };
  }

  function switchChat(id) {
    currentChatId = id;
    renderChatHistoryList();
    renderChatMessages();
    if (sidebarCollapsed && sidebarOverlayOpen) {
      closeSidebarOverlay();
    }
  }

  function getCurrentChat() {
    if (!currentChatId) return null;
    return chatSessions.find(function (c) { return c.id === currentChatId; });
  }

  function addMessageToCurrentChat(role, text, requestId, excelFiles, quickActions, thinking) {
    var chat = getCurrentChat();
    if (!chat) {
      createNewChat();
      chat = getCurrentChat();
    }
    if (!chat) return;
    var timestamp = Date.now();
    var msg = { role: role, text: text, requestId: requestId || null, timestamp: timestamp };
    if (excelFiles && excelFiles.length) {
      msg.excelFiles = excelFiles;
    }
    if (quickActions && quickActions.length) {
      msg.quickActions = quickActions;
    }
    if (thinking) {
      msg.thinking = thinking;
    }
    chat.messages.push(msg);
    if (role === "user" && chat.title === "Новий чат") {
      chat.title = text.length > 50 ? text.slice(0, 50) + "…" : text;
    }
    chat.updatedAt = timestamp;
    saveChatSessions();
    renderChatHistoryList();
  }

  function formatMessageTimestamp(ts) {
    if (!ts || typeof ts !== "number") return "";
    try {
      return new Date(ts).toLocaleString("uk-UA", {
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit"
      });
    } catch (e) {
      return "";
    }
  }

  function deleteChat(id, e) {
    if (e) {
      e.preventDefault();
      e.stopPropagation();
    }
    var idx = chatSessions.findIndex(function (c) { return c.id === id; });
    if (idx === -1) return;
    var chat = chatSessions[idx];
    var artifactIds = [];
    if (chat && chat.messages) {
      chat.messages.forEach(function (m) {
        if (m.excelFiles && m.excelFiles.length) {
          m.excelFiles.forEach(function (f) {
            var aid = f.artifact_id || f.artifactId;
            if (aid) artifactIds.push(aid);
          });
        }
      });
    }
    if (artifactIds.length) {
      fetch("/api/files/artifacts/delete", {
        method: "POST",
        headers: apiHeaders(),
        body: JSON.stringify({ artifact_ids: artifactIds })
      }).catch(function () {});
    }
    chatSessions.splice(idx, 1);
    if (currentChatId === id) {
      currentChatId = chatSessions.length > 0 ? chatSessions[0].id : null;
    }
    saveChatSessions();
    renderChatHistoryList();
    renderChatMessages();
    if (sidebarOverlayOpen) closeSidebarOverlay();
  }

  function renderChatHistoryList() {
    var list = document.getElementById("chat-history-list");
    var sidebar = document.getElementById("chat-sidebar");
    if (!list || !sidebar) return;
    list.innerHTML = "";
    if (chatSessions.length === 0) {
      var empty = document.createElement("div");
      empty.className = "chat-history-empty";
      empty.textContent = "Немає чатів";
      list.appendChild(empty);
      return;
    }
    chatSessions.forEach(function (s) {
      var item = document.createElement("div");
      item.className = "chat-history-item" + (s.id === currentChatId ? " active" : "");
      item.setAttribute("data-chat-id", s.id);
      var titleSpan = document.createElement("span");
      titleSpan.className = "chat-history-item-title";
      titleSpan.textContent = s.title || "Без назви";
      titleSpan.addEventListener("click", function () {
        switchChat(s.id);
      });
      var delBtn = document.createElement("button");
      delBtn.type = "button";
      delBtn.className = "chat-history-item-delete";
      delBtn.title = "Видалити чат";
      delBtn.innerHTML = "×";
      delBtn.addEventListener("click", function (e) {
        deleteChat(s.id, e);
      });
      item.appendChild(titleSpan);
      item.appendChild(delBtn);
      list.appendChild(item);
    });
  }

  function renderChatListingPinned() {
    var wrap = document.getElementById("chat-listing-pinned");
    if (!wrap) return;
    var chat = getCurrentChat();
    var lc = chat && chat.listingContext;
    if (!lc || (!lc.page_url && !lc.detail_id)) {
      wrap.classList.add("hidden");
      wrap.innerHTML = "";
      return;
    }
    wrap.classList.remove("hidden");
    var summary = (lc.summary || "").slice(0, 80);
    if (summary.length < (lc.summary || "").length) summary += "…";
    wrap.innerHTML = "";
    var label = document.createElement("span");
    label.className = "chat-listing-pinned-label";
    label.textContent = "Обговорюємо:";
    wrap.appendChild(label);
    var btn = document.createElement("button");
    btn.type = "button";
    btn.className = "chat-listing-pinned-link";
    btn.textContent = summary || "Оголошення";
    btn.title = "Відкрити оголошення";
    btn.onclick = function () {
      if (lc.detail_source && lc.detail_id) {
        showDetail(lc.detail_source, lc.detail_id);
      } else if (lc.page_url) {
        if (typeof window.open === "function") window.open(lc.page_url, "_blank");
      }
    };
    wrap.appendChild(btn);
  }

  function renderChatMessages() {
    renderChatListingPinned();
    var container = document.getElementById("chat-messages");
    if (!container) return;
    container.innerHTML = "";
    var chat = getCurrentChat();
    if (!chat || !chat.messages.length) return;
    chat.messages.forEach(function (m) {
      appendChatMessageDOM(container, m.role, m.text, m.requestId, m.timestamp, m.excelFiles, m.quickActions, m.thinking);
    });
    container.scrollTop = container.scrollHeight;
  }

  function appendChatMessageDOM(container, role, text, requestId, timestamp, excelFiles, quickActions, thinking) {
    if (!container) return;
    var div = document.createElement("div");
    div.className = "chat-msg " + role;
    var contentWrap = document.createElement("div");
    contentWrap.className = "chat-msg-content";
    if (role === "bot") {
      contentWrap.innerHTML = text || "";
      div.appendChild(contentWrap);
      if (thinking) {
        var thinkingWrap = document.createElement("details");
        thinkingWrap.className = "chat-msg-thinking";
        var thinkingSummary = document.createElement("summary");
        thinkingSummary.textContent = "Хід думок агента";
        thinkingSummary.className = "chat-msg-thinking-summary";
        thinkingWrap.appendChild(thinkingSummary);
        var thinkingContent = document.createElement("div");
        thinkingContent.className = "chat-msg-thinking-content";
        thinkingContent.textContent = thinking;
        thinkingWrap.appendChild(thinkingContent);
        div.appendChild(thinkingWrap);
      }
      if (quickActions && quickActions.length) {
        var qaWrap = document.createElement("div");
        qaWrap.className = "chat-quick-actions";
        quickActions.forEach(function (qa) {
          var btn = document.createElement("button");
          btn.type = "button";
          btn.className = "btn btn-small btn-secondary chat-quick-action-btn";
          btn.textContent = qa.label || qa.prompt;
          btn.dataset.prompt = qa.prompt || "";
          btn.onclick = function () {
            var p = (qa.prompt || btn.dataset.prompt || "").trim();
            if (p) sendChatMessageWithText(p);
            else {
              var inp = document.getElementById("chat-input");
              if (inp) inp.focus();
            }
          };
          qaWrap.appendChild(btn);
        });
        div.appendChild(qaWrap);
      }
      if (excelFiles && excelFiles.length) {
        excelFiles.forEach(function (f) {
          var aid = f.artifact_id || f.artifactId;
          var token = f.download_token || f.downloadToken;
          var btn = document.createElement("button");
          btn.type = "button";
          btn.className = "btn btn-small btn-primary chat-file-download-link";
          btn.textContent = "Отримати в чаті: " + (f.filename || "Excel");
          btn.onclick = function () {
            btn.disabled = true;
            btn.textContent = "Надсилання...";
            fetch("/api/files/send-artifact-via-bot", {
              method: "POST",
              headers: apiHeaders(),
              body: JSON.stringify({ artifact_id: aid, token: token })
            })
              .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, data: d }; }); })
              .then(function (x) {
                if (x.ok) {
                  btn.textContent = "Надіслано в чат";
                } else {
                  btn.disabled = false;
                  btn.textContent = "Отримати в чаті: " + (f.filename || "Excel");
                  alert(x.data.detail || "Помилка");
                }
              })
              .catch(function () {
                btn.disabled = false;
                btn.textContent = "Отримати в чаті: " + (f.filename || "Excel");
                alert("Помилка мережі");
              });
          };
          div.appendChild(btn);
        });
      }
      if (requestId) {
        var feedbackDiv = document.createElement("div");
        feedbackDiv.className = "chat-feedback";
        feedbackDiv.setAttribute("data-request-id", requestId);
        var likeBtn = document.createElement("button");
        likeBtn.className = "feedback-btn feedback-like";
        likeBtn.innerHTML = "👍";
        likeBtn.title = "Відповідь корисна";
        likeBtn.onclick = function () { submitFeedback(requestId, "like", text); };
        var dislikeBtn = document.createElement("button");
        dislikeBtn.className = "feedback-btn feedback-dislike";
        dislikeBtn.innerHTML = "👎";
        dislikeBtn.title = "Відповідь не корисна";
        dislikeBtn.onclick = function () { submitFeedback(requestId, "dislike", text); };
        feedbackDiv.appendChild(likeBtn);
        feedbackDiv.appendChild(dislikeBtn);
        div.appendChild(feedbackDiv);
      }
    } else {
      contentWrap.textContent = text;
      div.appendChild(contentWrap);
    }
    var tsFormatted = formatMessageTimestamp(timestamp);
    if (tsFormatted) {
      var timeSpan = document.createElement("span");
      timeSpan.className = "chat-msg-time";
      timeSpan.textContent = tsFormatted;
      div.appendChild(timeSpan);
    }
    container.appendChild(div);
    container.scrollTop = container.scrollHeight;
  }

  function show(id) {
    currentScreen = id || "screen-home";
    ["screen-loading", "screen-error", "screen-home", "screen-admin", "screen-files", "screen-search", "screen-detail"].forEach(function (sid) {
      var el = document.getElementById(sid);
      if (el) {
        if (sid === id) {
          el.classList.remove("hidden");
        } else {
          el.classList.add("hidden");
        }
      }
    });
    // Модалка фільтрів лише для екрану пошуку — при переході на інший екран закриваємо її
    if (id !== "screen-search") {
      var filterModal = document.getElementById("filter-builder-modal");
      if (filterModal) filterModal.classList.add("hidden");
    }
    updateSidebarState();
    setNavActive(currentScreen);
  }

  function setNavActive(screenId) {
    document.querySelectorAll(".nav a[data-screen], .nav button[data-screen]").forEach(function (el) {
      el.classList.toggle("active", el.getAttribute("data-screen") === screenId);
    });
  }

  function showError(msg) {
    var el = document.getElementById("screen-error");
    if (el) {
      el.textContent = msg;
      el.classList.remove("hidden");
    }
    show("screen-error");
  }

  function renderNav(me) {
    var nav = document.getElementById("nav");
    if (!nav) {
      console.error("nav element not found");
      return;
    }
    nav.innerHTML = "";

    if (me && me.authorized) {
      var aSearch = document.createElement("a");
      aSearch.href = "#";
      aSearch.textContent = "Пошук";
      aSearch.classList.add("secondary");
      aSearch.setAttribute("data-screen", "screen-search");
      aSearch.addEventListener("click", function (e) { 
        e.preventDefault(); 
        showSearch(); 
      });
      nav.appendChild(aSearch);
      
      var a1 = document.createElement("a");
      a1.href = "#";
      a1.textContent = "AI-асистент";
      a1.setAttribute("data-screen", "screen-home");
      a1.classList.add("secondary");
      a1.addEventListener("click", function (e) { 
        e.preventDefault(); 
        show("screen-home"); 
        var homeScreen = document.getElementById("screen-home");
        if (homeScreen) homeScreen.classList.remove("hidden");
      });
      nav.appendChild(a1);
      
      var a2 = document.createElement("a");
      a2.href = "#";
      a2.textContent = "Звіти";
      a2.classList.add("secondary");
      a2.setAttribute("data-screen", "screen-files");
      a2.addEventListener("click", function (e) { 
        e.preventDefault(); 
        showFiles(); 
      });
      nav.appendChild(a2);
      
      if (me.is_admin) {
        var a3 = document.createElement("a");
        a3.href = "#";
        a3.textContent = "Адміністрування";
        a3.classList.add("secondary");
        a3.setAttribute("data-screen", "screen-admin");
        a3.addEventListener("click", function (e) { 
          e.preventDefault(); 
          show("screen-admin"); 
          var adminScreen = document.getElementById("screen-admin");
          if (adminScreen) adminScreen.classList.remove("hidden");
          loadAdminIntegrityStatus();
          loadAdminCadastralStats();
          loadAdminOlxClickerStats();
          loadAdminUsageStats();
          loadAdminQueueStatus();
          loadAdminLlmRegions();
        });
        nav.appendChild(a3);
      }
    }
    
    var platform = Tg && (Tg.platform || "").toLowerCase();
    var isDesktop = platform === "web" || platform === "tdesktop" || platform === "macos" || platform === "windows" || platform === "weba" || platform === "webk";
    if (me && me.authorized && isDesktop && Tg && Tg.requestFullscreen) {
      var expandBtn = document.createElement("button");
      expandBtn.type = "button";
      expandBtn.className = "btn-expand-nav secondary";
      expandBtn.textContent = "⛶";
      expandBtn.title = "Розгорнути на весь екран";
      expandBtn.addEventListener("click", function () {
        try {
          if (Tg.isFullscreen && Tg.exitFullscreen) {
            Tg.exitFullscreen();
          } else if (Tg.requestFullscreen) {
            Tg.requestFullscreen();
          }
        } catch (e) {
          console.log("Fullscreen toggle error:", e);
        }
      });
      nav.appendChild(expandBtn);
    }
    
    setNavActive(currentScreen);
  }

  function appendChatMessage(role, text, requestId, excelFiles, quickActions, thinking) {
    addMessageToCurrentChat(role, text, requestId, excelFiles, quickActions, thinking);
    var container = document.getElementById("chat-messages");
    var chat = getCurrentChat();
    var lastMsg = chat && chat.messages.length ? chat.messages[chat.messages.length - 1] : null;
    var ts = lastMsg ? lastMsg.timestamp : null;
    appendChatMessageDOM(container, role, text, requestId, ts, excelFiles, quickActions, thinking);
  }
  
  function submitFeedback(requestId, feedbackType, responseText) {
    // Відключаємо кнопки після натискання
    var feedbackDiv = document.querySelector('[data-request-id="' + requestId + '"]');
    if (feedbackDiv) {
      var buttons = feedbackDiv.querySelectorAll(".feedback-btn");
      buttons.forEach(function(btn) {
        btn.disabled = true;
        btn.style.opacity = "0.5";
      });
    }
    
    // Отримуємо оригінальний запит користувача (останнє повідомлення user)
    var userMessages = document.querySelectorAll(".chat-msg.user");
    var userQuery = userMessages.length > 0 ? userMessages[userMessages.length - 1].textContent : "";
    
    var chat = getCurrentChat();
    var chatId = chat ? chat.id : null;
    fetch("/api/feedback/submit", {
      method: "POST",
      headers: apiHeaders(),
      body: JSON.stringify({
        request_id: requestId,
        feedback_type: feedbackType,
        user_query: userQuery,
        response_text: responseText,
        chat_id: chatId
      })
    })
      .then(function(r) {
        if (!r.ok) {
          return r.json().then(function(body) {
            throw new Error(body.detail || "Помилка відправки фідбеку");
          });
        }
        return r.json();
      })
      .then(function(data) {
        // Підсвічуємо натиснуту кнопку
        if (feedbackDiv) {
          var clickedBtn = feedbackDiv.querySelector(".feedback-" + feedbackType);
          if (clickedBtn) {
            clickedBtn.style.backgroundColor = feedbackType === "like" ? "#4CAF50" : "#f44336";
            clickedBtn.style.color = "white";
          }
        }
        
        // Якщо дизлайк і є результат діагностики, показуємо повідомлення
        if (feedbackType === "dislike" && data.diagnostic_result) {
          var diagnosticMsg = "Дякуємо за фідбек! Виявлено проблеми:\n";
          if (data.diagnostic_result.issues && data.diagnostic_result.issues.length > 0) {
            diagnosticMsg += data.diagnostic_result.issues.join("\n");
          }
          if (data.diagnostic_result.suggestions && data.diagnostic_result.suggestions.length > 0) {
            diagnosticMsg += "\n\nРекомендації:\n" + data.diagnostic_result.suggestions.join("\n");
          }
          // Можна показати в консолі або у вікні (для дебагу)
          console.log("Діагностика:", diagnosticMsg);
        }
      })
      .catch(function(err) {
        console.error("Помилка відправки фідбеку:", err);
        // Повертаємо кнопки в активний стан
        if (feedbackDiv) {
          var buttons = feedbackDiv.querySelectorAll(".feedback-btn");
          buttons.forEach(function(btn) {
            btn.disabled = false;
            btn.style.opacity = "1";
          });
        }
      });
  }

  var currentChatAbortController = null;

  var CAPABILITIES_PHRASES = [
    "що ти вмієш", "що ти можеш", "твої можливості", "розкажи про себе",
    "які у тебе функції", "допоможи з", "як ти можеш допомогти", "що вмієш"
  ];
  var CAPABILITIES_RESPONSE = "Я — AI-помічник для роботи з оголошеннями нерухомості (OLX та ProZorro).\n\n" +
    "Можу:\n" +
    "• Шукати оголошення за параметрами (регіон, місто, ціна, площа, тип)\n" +
    "• Порівнювати ціни, аналізувати ринок\n" +
    "• Експортувати звіти в Excel (за період, геообмеженням)\n" +
    "• Аналізувати конкретне оголошення (якщо воно відкрите в застосунку)\n\n" +
    "Спробуйте один із прикладів нижче:";
  var CAPABILITIES_QUICK_ACTIONS = [
    { label: "Пошук приміщення в Києві", prompt: "Знайди приміщення для аптеки в Києві до 100 м², 1 поверх" },
    { label: "Звіт за тиждень у файл", prompt: "Вивантаж звіт по оголошеннях за останній тиждень у Excel" },
    { label: "Ціни за м² по областях", prompt: "Порівняй середню ціну за м² по областях України" },
    { label: "Оголошення OLX у Львові", prompt: "Покажи оголошення OLX у Львові за останній тиждень" }
  ];

  function isCapabilitiesQuery(text) {
    if (!text || typeof text !== "string") return false;
    var t = text.toLowerCase().trim();
    return CAPABILITIES_PHRASES.some(function (p) { return t.indexOf(p) >= 0; });
  }

  function sendChatMessageWithText(text) {
    if (!text || !text.trim()) return;
    if (!getCurrentChat()) createNewChat();
    appendChatMessage("user", text.trim());
    var statusEl = appendStatusBelowUser("Підключення...");
    setInputState(true);
    var abortController = new AbortController();
    currentChatAbortController = abortController;
    var messagesContainer = document.getElementById("chat-messages");
    var chat = getCurrentChat();
    var chatId = chat ? chat.id : null;
    var listingContext = chat && chat.listingContext ? chat.listingContext : null;
    fetch("/api/llm/chat-stream", {
      method: "POST",
      headers: apiHeaders(),
      body: JSON.stringify({ text: text.trim(), chat_id: chatId, listing_context: listingContext, reply_to_text: null }),
      signal: abortController.signal,
    })
      .then(function (r) {
        if (!r.ok) {
          return r.json().then(function (body) {
            var msg = (body && body.detail) || (r.status === 403 ? "Немає доступу" : "Помилка " + r.status);
            throw new Error(typeof msg === "string" ? msg : JSON.stringify(msg));
          }, function () {
            throw new Error(r.status === 403 ? "Немає доступу" : "Помилка " + r.status);
          });
        }
        if (!r.body || !r.body.getReader) throw new Error("Стрімінг не підтримується");
        return r.body.getReader();
      })
      .then(function (reader) {
        var decoder = new TextDecoder();
        var buffer = "";
        function processEvent(eventData) {
          try {
            var data = JSON.parse(eventData);
            if (data.type === "status" && statusEl) {
              statusEl.textContent = data.message || "Обробка...";
              if (messagesContainer) messagesContainer.scrollTop = messagesContainer.scrollHeight;
            } else if (data.type === "done") {
              currentChatAbortController = null;
              setInputState(false);
              if (statusEl && statusEl.parentNode) statusEl.remove();
              var reqId = data.request_id || null;
              var excelFiles = (data.excel_files || []).filter(function (f) { return f.artifact_id && f.download_token; });
              var qa = data.quick_actions || [];
              var thinking = data.thinking || null;
              appendChatMessage("bot", data.response || "", reqId, excelFiles, qa.length ? qa : undefined, thinking);
              return true;
            } else if (data.type === "error") {
              currentChatAbortController = null;
              setInputState(false);
              if (statusEl && statusEl.parentNode) statusEl.remove();
              appendChatMessage("bot", "Помилка: " + (data.message || "Помилка"));
              return true;
            }
          } catch (e) {}
          return false;
        }
        return reader.read().then(function processChunk(result) {
          if (result.done) {
            currentChatAbortController = null;
            setInputState(false);
            if (statusEl && statusEl.parentNode) statusEl.remove();
            appendChatMessage("bot", "Потік завершено без відповіді");
            return;
          }
          buffer += decoder.decode(result.value, { stream: true });
          var events = buffer.split("\n\n");
          buffer = events.pop() || "";
          for (var i = 0; i < events.length; i++) {
            var lines = events[i].split("\n");
            for (var j = 0; j < lines.length; j++) {
              if (lines[j].indexOf("data: ") === 0) {
                var jsonStr = lines[j].slice(6).trim();
                if (jsonStr && jsonStr !== "[DONE]") {
                  if (processEvent(jsonStr)) return;
                }
                break;
              }
            }
          }
          return reader.read().then(processChunk);
        });
      })
      .catch(function (err) {
        currentChatAbortController = null;
        setInputState(false);
        if (statusEl && statusEl.parentNode) statusEl.remove();
        appendChatMessage("bot", "Помилка: " + (err.message || err));
      });
  }

  function appendStatusBelowUser(message) {
    var container = document.getElementById("chat-messages");
    if (!container) return null;
    var statusEl = document.createElement("div");
    statusEl.className = "chat-msg-status";
    statusEl.textContent = message || "Обробка...";
    container.appendChild(statusEl);
    container.scrollTop = container.scrollHeight;
    return statusEl;
  }

  function setInputState(processing) {
    var input = document.getElementById("chat-input");
    var sendBtn = document.getElementById("chat-send");
    var stopBtn = document.getElementById("chat-stop");
    if (input) input.disabled = processing;
    if (sendBtn) sendBtn.classList.toggle("hidden", processing);
    if (stopBtn) stopBtn.classList.toggle("hidden", !processing);
  }

  function sendChatMessage() {
    var input = document.getElementById("chat-input");
    var text = input && input.value.trim();
    if (!text) return;
    if (!getCurrentChat()) createNewChat();
    appendChatMessage("user", text);
    input.value = "";
    if (isCapabilitiesQuery(text)) {
      appendChatMessage("bot", CAPABILITIES_RESPONSE, null, null, CAPABILITIES_QUICK_ACTIONS);
      return;
    }
    var statusEl = appendStatusBelowUser("Підключення...");
    setInputState(true);

    var abortController = new AbortController();
    currentChatAbortController = abortController;

    var messagesContainer = document.getElementById("chat-messages");

    function finish(err, data) {
      currentChatAbortController = null;
      setInputState(false);
      if (statusEl && statusEl.parentNode) statusEl.remove();
      if (err) {
        appendChatMessage("bot", "Помилка: " + (err.message || err));
      } else if (data) {
        var requestId = data.request_id || null;
        var excelFiles = (data.excel_files || []).filter(function (f) {
          return f.artifact_id && f.download_token;
        });
        var qa = data.quick_actions || [];
        appendChatMessage("bot", data.response || "", requestId, excelFiles, qa.length ? qa : undefined);
      }
    }

    var chat = getCurrentChat();
    var chatId = chat ? chat.id : null;
    var listingContext = chat && chat.listingContext ? chat.listingContext : null;
    fetch("/api/llm/chat-stream", {
      method: "POST",
      headers: apiHeaders(),
      body: JSON.stringify({ text: text, chat_id: chatId, listing_context: listingContext, reply_to_text: null }),
      signal: abortController.signal,
    })
      .then(function (r) {
        if (!r.ok) {
          return r.json().then(function (body) {
            var msg = (body && body.detail) || (r.status === 403 ? "Немає доступу" : "Помилка " + r.status);
            throw new Error(typeof msg === "string" ? msg : JSON.stringify(msg));
          }, function () {
            throw new Error(r.status === 403 ? "Немає доступу" : "Помилка " + r.status);
          });
        }
        if (!r.body || !r.body.getReader) {
          throw new Error("Стрімінг не підтримується");
        }
        return r.body.getReader();
      })
      .then(function (reader) {
        var decoder = new TextDecoder();
        var buffer = "";
        function processEvent(eventData) {
          try {
            var data = JSON.parse(eventData);
            if (data.type === "status" && statusEl) {
              statusEl.textContent = data.message || "Обробка...";
              if (messagesContainer) messagesContainer.scrollTop = messagesContainer.scrollHeight;
            } else if (data.type === "done") {
              finish(null, data);
              return true;
            } else if (data.type === "error") {
              finish(new Error(data.message || "Помилка"));
              return true;
            }
          } catch (e) {}
          return false;
        }
        return reader.read().then(function processChunk(result) {
          if (result.done) {
            finish(new Error("Потік завершено без відповіді"));
            return;
          }
          buffer += decoder.decode(result.value, { stream: true });
          var events = buffer.split("\n\n");
          buffer = events.pop() || "";
          for (var i = 0; i < events.length; i++) {
            var lines = events[i].split("\n");
            for (var j = 0; j < lines.length; j++) {
              if (lines[j].indexOf("data: ") === 0) {
                var jsonStr = lines[j].slice(6).trim();
                if (jsonStr && jsonStr !== "[DONE]") {
                  if (processEvent(jsonStr)) return;
                }
                break;
              }
            }
          }
          return reader.read().then(processChunk);
        });
      })
      .catch(function (err) {
        if (err.name === "AbortError") {
          finish(new Error("Запит перервано"));
        } else {
          finish(err);
        }
      });
  }

  function stopChatRequest() {
    if (currentChatAbortController) {
      currentChatAbortController.abort();
    }
  }

  function downloadReport(days) {
    var wrap = document.getElementById("files-report-link-wrap");
    if (wrap) {
      wrap.classList.remove("hidden");
      wrap.innerHTML = "<span class=\"file-download-hint\">Надсилання в чат...</span>";
    }
    fetch("/api/files/send-report-via-bot", {
      method: "POST",
      headers: apiHeaders(),
      body: JSON.stringify({ days: days })
    })
      .then(function (r) {
        if (!r.ok) return r.json().then(function (d) { throw new Error(d.detail || "Помилка"); });
        return r.json();
      })
      .then(function (data) {
        if (wrap) {
          wrap.innerHTML = "<span class=\"file-download-success\">" + (data.message || "Файл надіслано в чат бота") + "</span>";
        }
      })
      .catch(function (err) {
        if (wrap) {
          wrap.innerHTML = "<span class=\"file-download-error\">" + (err.message || "Не вдалося надіслати") + "</span>";
        } else {
          alert(err.message || "Не вдалося надіслати");
        }
      });
  }

  var reportTemplates = [];
  var reportConstructorPrefill = null;

  function showFiles() {
    show("screen-files");
    var filesScreen = document.getElementById("screen-files");
    if (filesScreen) filesScreen.classList.remove("hidden");
    loadReportTemplates();
  }

  function loadReportTemplates() {
    fetch("/api/report-templates/", { headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) throw new Error("Помилка завантаження");
        return r.json();
      })
      .then(function (data) {
        reportTemplates = data.templates || [];
        renderReportTemplates();
      })
      .catch(function (err) {
        console.error("Report templates load error:", err);
        reportTemplates = [];
        renderReportTemplates();
      });
  }

  function renderReportTemplates() {
    var listEl = document.getElementById("report-templates-list");
    if (!listEl) return;
    listEl.innerHTML = "";
    reportTemplates.forEach(function (t) {
      var item = document.createElement("div");
      item.className = "report-template-item";
      item.draggable = !t.is_default;
      item.dataset.id = t._id;
      if (t.is_default) item.dataset.isDefault = "true";
      var nameSpan = document.createElement("span");
      nameSpan.className = "report-template-name";
      nameSpan.textContent = t.name || "Без назви";
      item.appendChild(nameSpan);
      var actions = document.createElement("div");
      actions.className = "report-template-actions";
      var genBtn = document.createElement("button");
      genBtn.type = "button";
      genBtn.className = "btn btn-small btn-primary";
      genBtn.textContent = "Згенерувати";
      genBtn.addEventListener("click", function () { generateReportFromTemplate(t._id); });
      actions.appendChild(genBtn);
      if (!t.is_default) {
        var delBtn = document.createElement("button");
        delBtn.type = "button";
        delBtn.className = "btn btn-small btn-danger";
        delBtn.textContent = "Видалити";
        delBtn.addEventListener("click", function () { deleteReportTemplate(t._id); });
        actions.appendChild(delBtn);
      }
      item.appendChild(actions);
      listEl.appendChild(item);
    });
    if (typeof Sortable !== "undefined") {
      new Sortable(listEl, {
        animation: 150,
        handle: ".report-template-name",
        filter: ".report-template-item[data-is-default=true]",
        onEnd: function (evt) {
          var ids = [];
          listEl.querySelectorAll(".report-template-item").forEach(function (el) {
            if (el.dataset.id) ids.push(el.dataset.id);
          });
          if (ids.length > 0) {
            fetch("/api/report-templates/reorder", {
              method: "POST",
              headers: apiHeaders(),
              body: JSON.stringify({ template_ids: ids })
            }).then(function () { loadReportTemplates(); });
          }
        }
      });
    }
  }

  function generateReportFromTemplate(templateId) {
    var statusEl = document.getElementById("files-generate-status");
    if (statusEl) {
      statusEl.classList.remove("hidden");
      statusEl.textContent = "Генерація звіту...";
    }
    fetch("/api/report-templates/" + encodeURIComponent(templateId) + "/generate", {
      method: "POST",
      headers: apiHeaders(),
      body: JSON.stringify({ send_via_bot: true })
    })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (!data.success) {
          if (statusEl) statusEl.textContent = data.error || data.detail || "Помилка";
          return;
        }
        if (data.message) {
          if (statusEl) statusEl.textContent = data.message + " (" + (data.rows_count || 0) + " рядків)";
        } else if (data.download_url) {
          if (statusEl) {
            statusEl.innerHTML = "Готово (" + (data.rows_count || 0) + " рядків). ";
            var a = document.createElement("a");
            a.href = data.download_url;
            a.download = "Звіт.xlsx";
            a.className = "btn btn-primary file-download-link";
            a.textContent = "Скачати";
            statusEl.appendChild(a);
          }
        } else if (data.data) {
          var blob = base64ToBlob(data.data, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
          var a = document.createElement("a");
          a.href = URL.createObjectURL(blob);
          a.download = "Звіт.xlsx";
          a.click();
          URL.revokeObjectURL(a.href);
          if (statusEl) statusEl.textContent = "Готово (" + (data.rows_count || 0) + " рядків)";
        }
      })
      .catch(function (err) {
        if (statusEl) statusEl.textContent = err.message || "Помилка";
      });
  }

  function base64ToBlob(b64, mime) {
    var binary = atob(b64);
    var bytes = new Uint8Array(binary.length);
    for (var i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return new Blob([bytes], { type: mime });
  }

  function deleteReportTemplate(templateId) {
    if (!confirm("Видалити шаблон?")) return;
    fetch("/api/report-templates/" + encodeURIComponent(templateId), {
      method: "DELETE",
      headers: apiHeaders()
    })
      .then(function (r) {
        if (!r.ok) return r.json().then(function (d) { throw new Error(d.detail || "Помилка"); });
        loadReportTemplates();
      })
      .catch(function (err) { alert(err.message); });
  }

  function openReportConstructor(prefill) {
    reportConstructorPrefill = prefill || null;
    var modal = document.getElementById("report-constructor-modal");
    if (!modal) return;
    loadFilterOptions(prefill ? prefill.region : null);
    if (prefill) {
      document.getElementById("constructor-source").value = prefill.source || "";
      document.getElementById("constructor-date-filter").value = String(prefill.date_filter || 7);
      document.getElementById("constructor-region").value = prefill.region || "";
      document.getElementById("constructor-city").value = prefill.city || "";
      document.getElementById("constructor-property-type").value = prefill.property_type || "";
      document.getElementById("constructor-name").value = prefill.name || "";
    } else {
      document.getElementById("constructor-source").value = "";
      document.getElementById("constructor-date-filter").value = "7";
      document.getElementById("constructor-region").value = "";
      document.getElementById("constructor-city").value = "";
      document.getElementById("constructor-property-type").value = "";
      document.getElementById("constructor-name").value = "";
    }
    modal.classList.remove("hidden");
  }

  function closeReportConstructor() {
    var modal = document.getElementById("report-constructor-modal");
    if (modal) modal.classList.add("hidden");
    reportConstructorPrefill = null;
  }

  function getConstructorParams() {
    var priceOp = document.getElementById("constructor-price-op").value;
    var priceVal = document.getElementById("constructor-price-value").value;
    var priceM2Op = document.getElementById("constructor-price-m2-op").value;
    var priceM2Val = document.getElementById("constructor-price-m2-value").value;
    var priceHaOp = document.getElementById("constructor-price-ha-op").value;
    var priceHaVal = document.getElementById("constructor-price-ha-value").value;
    var params = {
      source: document.getElementById("constructor-source").value,
      date_filter: parseInt(document.getElementById("constructor-date-filter").value, 10) || 7,
      region: document.getElementById("constructor-region").value.trim() || null,
      city: document.getElementById("constructor-city").value.trim() || null,
      property_type: document.getElementById("constructor-property-type").value || null,
      sort_field: document.getElementById("constructor-sort-field").value,
      sort_order: document.getElementById("constructor-sort-order").value,
      output_format: document.getElementById("constructor-output-format").value
    };
    if (priceOp && priceVal) {
      params.price = { op: priceOp, value: parseFloat(priceVal), currency: document.getElementById("constructor-price-currency").value };
    }
    if (priceM2Op && priceM2Val) {
      params.price_per_m2 = { op: priceM2Op, min: priceM2Op === "gte" ? parseFloat(priceM2Val) : null, max: priceM2Op === "lte" ? parseFloat(priceM2Val) : null, currency: document.getElementById("constructor-price-m2-currency").value };
    }
    if (priceHaOp && priceHaVal) {
      params.price_per_ha = { op: priceHaOp, min: priceHaOp === "gte" ? parseFloat(priceHaVal) : null, max: priceHaOp === "lte" ? parseFloat(priceHaVal) : null, currency: document.getElementById("constructor-price-ha-currency").value };
    }
    return params;
  }

  function initReportTemplates() {
    var addBtn = document.getElementById("report-add-template");
    if (addBtn) addBtn.addEventListener("click", function () { openReportConstructor(null); });
    var saveBtn = document.getElementById("constructor-save");
    if (saveBtn) saveBtn.addEventListener("click", function () {
      var name = document.getElementById("constructor-name").value.trim();
      if (!name) { alert("Введіть назву шаблону"); return; }
      var params = getConstructorParams();
      fetch("/api/report-templates/", {
        method: "POST",
        headers: apiHeaders(),
        body: JSON.stringify({ name: name, params: params })
      })
        .then(function (r) { return r.json(); })
        .then(function () {
          closeReportConstructor();
          loadReportTemplates();
        })
        .catch(function (err) { alert(err.message || "Помилка"); });
    });
    var cancelBtn = document.getElementById("constructor-cancel");
    if (cancelBtn) cancelBtn.addEventListener("click", closeReportConstructor);
    var genNameBtn = document.getElementById("constructor-generate-name");
    if (genNameBtn) genNameBtn.addEventListener("click", function () {
      var params = getConstructorParams();
      fetch("/api/report-templates/generate-name", {
        method: "POST",
        headers: apiHeaders(),
        body: JSON.stringify({ params: params })
      })
        .then(function (r) { return r.json(); })
        .then(function (d) {
          var nameEl = document.getElementById("constructor-name");
          if (nameEl && d.name) nameEl.value = d.name;
        });
    });
    var constRegion = document.getElementById("constructor-region");
    var constCity = document.getElementById("constructor-city");
    var constRegionDrop = document.getElementById("constructor-region-dropdown");
    var constCityDrop = document.getElementById("constructor-city-dropdown");
    if (constRegion) {
      constRegion.addEventListener("focus", function () {
        renderConstructorDropdown("constructor-region", "constructor-region-dropdown", filterOptions.regions, constRegion.value);
        if (constRegionDrop) constRegionDrop.classList.remove("hidden");
      });
      constRegion.addEventListener("input", function () {
        renderConstructorDropdown("constructor-region", "constructor-region-dropdown", filterOptions.regions, constRegion.value);
      });
    }
    if (constCity) {
      constCity.addEventListener("focus", function () {
        renderConstructorDropdown("constructor-city", "constructor-city-dropdown", filterOptions.cities, constCity.value);
        if (constCityDrop) constCityDrop.classList.remove("hidden");
      });
      constCity.addEventListener("input", function () {
        renderConstructorDropdown("constructor-city", "constructor-city-dropdown", filterOptions.cities, constCity.value);
      });
    }
    if (constRegionDrop) {
      constRegionDrop.addEventListener("click", function (e) {
        var item = e.target.closest(".filter-dropdown-item");
        if (item && item.dataset.value !== undefined) {
          constRegion.value = item.dataset.value;
          constRegionDrop.classList.add("hidden");
          filterLoadingState.cities = false;
          loadFilterOptions(constRegion.value);
        }
      });
    }
    if (constCityDrop) {
      constCityDrop.addEventListener("click", function (e) {
        var item = e.target.closest(".filter-dropdown-item");
        if (item && item.dataset.value !== undefined) {
          constCity.value = item.dataset.value;
          constCityDrop.classList.add("hidden");
        }
      });
    }
  }

  function startGenerateSeven() {
    var statusEl = document.getElementById("files-generate-status");
    if (statusEl) {
      statusEl.classList.remove("hidden");
      statusEl.textContent = "Запуск формування...";
    }
    fetch("/api/files/generate?days=7", { method: "POST", headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) throw new Error("Помилка " + r.status);
        return r.json();
      })
      .then(function (data) {
        var taskId = data.task_id;
        function poll() {
          fetch("/api/files/generate/status?task_id=" + encodeURIComponent(taskId), { headers: apiHeaders() })
            .then(function (res) { return res.json(); })
            .then(function (st) {
              if (statusEl) statusEl.textContent = st.message || st.status;
              if (st.status === "done") {
                var token = st.download_token || (st.download_url ? (st.download_url.match(/token=([^&]+)/) || [])[1] : null);
                if (token) {
                  statusEl.innerHTML = (st.message || "") + " ";
                  var sendBtn = document.createElement("button");
                  sendBtn.type = "button";
                  sendBtn.className = "btn btn-primary";
                  sendBtn.textContent = "Надіслати в чат";
                  sendBtn.onclick = function () {
                    sendBtn.disabled = true;
                    sendBtn.textContent = "Надсилання...";
                    fetch("/api/files/send-generated-via-bot", {
                      method: "POST",
                      headers: apiHeaders(),
                      body: JSON.stringify({ task_id: taskId, token: token })
                    })
                      .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, data: d }; }); })
                      .then(function (x) {
                        if (x.ok) sendBtn.textContent = "Надіслано в чат";
                        else { sendBtn.disabled = false; sendBtn.textContent = "Надіслати в чат"; alert(x.data.detail || "Помилка"); }
                      })
                      .catch(function () { sendBtn.disabled = false; sendBtn.textContent = "Надіслати в чат"; alert("Помилка мережі"); });
                  };
                  statusEl.appendChild(sendBtn);
                } else if (st.download_url) {
                  statusEl.innerHTML = (st.message || "") + " ";
                  var a = document.createElement("a");
                  a.href = st.download_url;
                  a.download = st.filename || "report.zip";
                  a.className = "btn btn-primary file-download-link";
                  a.textContent = "Скачати файл";
                  statusEl.appendChild(a);
                }
                return;
              }
              if (st.status === "error") return;
              setTimeout(poll, 2000);
            });
        }
        poll();
      })
      .catch(function (err) {
        if (statusEl) {
          statusEl.textContent = err.message || "Помилка";
          statusEl.classList.remove("hidden");
        }
      });
  }

  function adminAddUser() {
    var uid = document.getElementById("admin-add-user-id");
    var nick = document.getElementById("admin-add-nickname");
    var role = document.getElementById("admin-add-role");
    if (!uid || !nick || !role) return;
    var userId = parseInt(uid.value, 10);
    if (isNaN(userId)) { alert("Введіть коректний ID"); return; }
    fetch("/api/admin/add-user", {
      method: "POST",
      headers: apiHeaders(),
      body: JSON.stringify({ user_id: userId, role: role.value, nickname: nick.value.trim() })
    })
      .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, data: d }; }); })
      .then(function (x) {
        if (x.ok) alert(x.data.message || "Готово");
        else alert(x.data.detail || "Помилка");
      })
      .catch(function () { alert("Помилка мережі"); });
  }

  function adminBlockUser() {
    var uid = document.getElementById("admin-block-user-id");
    if (!uid) return;
    var userId = parseInt(uid.value, 10);
    if (isNaN(userId)) { alert("Введіть коректний ID"); return; }
    fetch("/api/admin/block-user", {
      method: "POST",
      headers: apiHeaders(),
      body: JSON.stringify({ user_id: userId })
    })
      .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, data: d }; }); })
      .then(function (x) {
        if (x.ok) alert(x.data.message || "Готово");
        else alert(x.data.detail || "Помилка");
      })
      .catch(function () { alert("Помилка мережі"); });
  }

  function adminDownloadProzorro() {
    fetch("/api/admin/prozorro-config?download=1", { headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) throw new Error("Помилка " + r.status);
        return r.blob();
      })
      .then(function (blob) {
        var a = document.createElement("a");
        a.href = URL.createObjectURL(blob);
        a.download = "ProZorro_clasification_codes.yaml";
        a.click();
        URL.revokeObjectURL(a.href);
      })
      .catch(function (err) { alert(err.message || "Помилка"); });
  }

  function adminUploadProzorro() {
    var input = document.getElementById("admin-upload-prozorro");
    if (!input || !input.files || !input.files[0]) { alert("Оберіть YAML файл"); return; }
    var form = new FormData();
    form.append("file", input.files[0]);
    var h = {};
    if (initData) h["X-Telegram-Init-Data"] = initData;
    fetch("/api/admin/prozorro-config", {
      method: "POST",
      headers: h,
      body: form
    })
      .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, data: d }; }); })
      .then(function (x) {
        if (x.ok) alert(x.data.message || "Готово");
        else alert(x.data.detail || "Помилка");
        input.value = "";
      })
      .catch(function () { alert("Помилка мережі"); });
  }

  function adminExportConfig() {
    fetch("/api/admin/export/config", { headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) throw new Error("Помилка " + r.status);
        var cd = r.headers.get("Content-Disposition") || "";
        var m = cd.match(/filename="?([^";\n]+)"?/);
        var name = (m && m[1]) ? m[1] : "pazuzu_config.zip";
        return r.blob().then(function (blob) { return { blob: blob, name: name }; });
      })
      .then(function (x) {
        var a = document.createElement("a");
        a.href = URL.createObjectURL(x.blob);
        a.download = x.name;
        a.click();
        URL.revokeObjectURL(a.href);
      })
      .catch(function (err) { alert(err.message || "Помилка"); });
  }

  function adminExportData() {
    fetch("/api/admin/export/data?limit=10000", { headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) throw new Error("Помилка " + r.status);
        var cd = r.headers.get("Content-Disposition") || "";
        var m = cd.match(/filename="?([^";\n]+)"?/);
        var name = (m && m[1]) ? m[1] : "pazuzu_data.zip";
        return r.blob().then(function (blob) { return { blob: blob, name: name }; });
      })
      .then(function (x) {
        var a = document.createElement("a");
        a.href = URL.createObjectURL(x.blob);
        a.download = x.name;
        a.click();
        URL.revokeObjectURL(a.href);
      })
      .catch(function (err) { alert(err.message || "Помилка"); });
  }

  function adminExportFull() {
    fetch("/api/admin/export/full?limit=5000", { headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) throw new Error("Помилка " + r.status);
        var cd = r.headers.get("Content-Disposition") || "";
        var m = cd.match(/filename="?([^";\n]+)"?/);
        var name = (m && m[1]) ? m[1] : "pazuzu_full.zip";
        return r.blob().then(function (blob) { return { blob: blob, name: name }; });
      })
      .then(function (x) {
        var a = document.createElement("a");
        a.href = URL.createObjectURL(x.blob);
        a.download = x.name;
        a.click();
        URL.revokeObjectURL(a.href);
      })
      .catch(function (err) { alert(err.message || "Помилка"); });
  }

  function adminImportConfig() {
    var input = document.getElementById("admin-import-config");
    if (!input || !input.files || !input.files[0]) { alert("Оберіть ZIP файл"); return; }
    var form = new FormData();
    form.append("file", input.files[0]);
    var h = {};
    if (initData) h["X-Telegram-Init-Data"] = initData;
    fetch("/api/admin/import/config", {
      method: "POST",
      headers: h,
      body: form
    })
      .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, data: d }; }); })
      .then(function (x) {
        if (x.ok) alert(x.data.message || "Готово. Перезапустіть застосунок для застосування.");
        else alert(x.data.detail || "Помилка");
        input.value = "";
      })
      .catch(function () { alert("Помилка мережі"); });
  }

  function loadAdminIntegrityStatus() {
    var el = document.getElementById("admin-integrity-status");
    if (!el) return;
    el.textContent = "Завантаження...";
    fetch("/api/admin/integrity/check", { headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) throw new Error("Помилка " + r.status);
        return r.json();
      })
      .then(function (d) {
        var cv = d.config_version || "—";
        var pv = d.platform_version || "—";
        var st = d.status || "ok";
        var stLabel = st === "ok" ? "OK" : (st === "warnings" ? "Попередження" : "Помилки");
        var parts = ["Конфіг: v" + cv + ", платформа: v" + pv + ", цілісність: " + stLabel];
        if (d.errors && d.errors.length) parts.push(" Помилки: " + d.errors.join("; "));
        if (d.warnings && d.warnings.length) parts.push(" Попередження: " + d.warnings.join("; "));
        el.textContent = parts.join("");
      })
      .catch(function (err) {
        el.textContent = "Помилка: " + (err.message || "невідома");
      });
  }

  function bindEvents(me) {
    loadChatSessions();
    if (chatSessions.length > 0 && !currentChatId) {
      currentChatId = chatSessions[0].id;
    }
    renderChatHistoryList();
    initSidebarCollapse();
    var sendBtn = document.getElementById("chat-send");
    if (sendBtn) sendBtn.addEventListener("click", sendChatMessage);
    var stopBtn = document.getElementById("chat-stop");
    if (stopBtn) stopBtn.addEventListener("click", stopChatRequest);
    var chatInput = document.getElementById("chat-input");
    if (chatInput) chatInput.addEventListener("keydown", function (e) {
      if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendChatMessage(); }
    });
    var newChatBtn = document.getElementById("sidebar-new-chat");
    if (newChatBtn) newChatBtn.addEventListener("click", createNewChat);

    var chatMessagesEl = document.getElementById("chat-messages");
    if (chatMessagesEl) {
      chatMessagesEl.addEventListener("click", function (e) {
        var link = e.target.closest("a.chat-link-internal");
        if (link && link.dataset.source && link.dataset.sourceId) {
          e.preventDefault();
          openListingInApp(link.dataset.source, link.dataset.sourceId);
        }
      });
    }

    initReportTemplates();

    var addBtn = document.getElementById("admin-add-btn");
    if (addBtn) addBtn.addEventListener("click", adminAddUser);
    var blockBtn = document.getElementById("admin-block-btn");
    if (blockBtn) blockBtn.addEventListener("click", adminBlockUser);
    var dlProzorro = document.getElementById("admin-download-prozorro");
    if (dlProzorro) dlProzorro.addEventListener("click", adminDownloadProzorro);
    var uploadBtn = document.getElementById("admin-upload-prozorro-btn");
    var uploadInput = document.getElementById("admin-upload-prozorro");
    if (uploadBtn && uploadInput) uploadBtn.addEventListener("click", function () { uploadInput.click(); });
    if (uploadInput) uploadInput.addEventListener("change", adminUploadProzorro);
    var exportConfigBtn = document.getElementById("admin-export-config");
    if (exportConfigBtn) exportConfigBtn.addEventListener("click", adminExportConfig);
    var exportDataBtn = document.getElementById("admin-export-data");
    if (exportDataBtn) exportDataBtn.addEventListener("click", adminExportData);
    var exportFullBtn = document.getElementById("admin-export-full");
    if (exportFullBtn) exportFullBtn.addEventListener("click", adminExportFull);
    var importConfigBtn = document.getElementById("admin-import-config-btn");
    var importConfigInput = document.getElementById("admin-import-config");
    if (importConfigBtn && importConfigInput) {
      importConfigBtn.addEventListener("click", function () { importConfigInput.click(); });
      importConfigInput.addEventListener("change", adminImportConfig);
    }
    initAdminLlmRegions();
    if (typeof initAdminSubtabs === "function") {
      initAdminSubtabs();
    }
    if (me && me.is_admin) {
      var btn1 = document.getElementById("admin-data-update-1");
      var btn7 = document.getElementById("admin-data-update-7");
      var btn30 = document.getElementById("admin-data-update-30");
      var btnFullOlx = document.getElementById("admin-data-update-full-olx");
      var btnFullProzorro = document.getElementById("admin-data-update-full-prozorro");
      if (btn1) btn1.addEventListener("click", function () { startAdminDataUpdate({ days: 1 }); });
      if (btn7) btn7.addEventListener("click", function () { startAdminDataUpdate({ days: 7 }); });
      if (btn30) btn30.addEventListener("click", function () { startAdminDataUpdate({ days: 30 }); });
      if (btnFullOlx) btnFullOlx.addEventListener("click", function () { startAdminDataUpdate({ mode: "full_olx" }); });
      if (btnFullProzorro) btnFullProzorro.addEventListener("click", function () { startAdminDataUpdate({ mode: "full_prozorro" }); });
      initAdminTargetedUpdate();
      var cadastral10 = document.getElementById("admin-cadastral-start-10");
      var cadastral50 = document.getElementById("admin-cadastral-start-50");
      var cadastralUnlimited = document.getElementById("admin-cadastral-start-unlimited");
      var cadastralReset = document.getElementById("admin-cadastral-reset");
      var cadastralResetStale = document.getElementById("admin-cadastral-reset-stale");
      if (cadastral10) cadastral10.addEventListener("click", function () { startAdminCadastralScraper(10); });
      if (cadastral50) cadastral50.addEventListener("click", function () { startAdminCadastralScraper(50); });
      if (cadastralUnlimited) cadastralUnlimited.addEventListener("click", function () { startAdminCadastralScraper(null); });
      if (cadastralResetStale) cadastralResetStale.addEventListener("click", adminCadastralResetStale);
      if (cadastralReset) cadastralReset.addEventListener("click", adminCadastralResetCells);
      var cadastralBuildIndex = document.getElementById("admin-cadastral-build-index");
      var cadastralBuildClusters = document.getElementById("admin-cadastral-build-clusters");
      var cadastralClearClusters = document.getElementById("admin-cadastral-clear-clusters");
      if (cadastralBuildIndex) cadastralBuildIndex.addEventListener("click", startAdminCadastralBuildIndex);
      if (cadastralBuildClusters) cadastralBuildClusters.addEventListener("click", startAdminCadastralBuildClusters);
      if (cadastralClearClusters) cadastralClearClusters.addEventListener("click", adminCadastralClearClusters);
      var rebuildAnalyticsBtn = document.getElementById("admin-rebuild-analytics");
      if (rebuildAnalyticsBtn) rebuildAnalyticsBtn.addEventListener("click", adminRebuildAnalytics);
      var anomalousPricesBtn = document.getElementById("admin-process-anomalous-prices");
      if (anomalousPricesBtn) anomalousPricesBtn.addEventListener("click", adminProcessAnomalousPrices);
      var olxHealthBtn = document.getElementById("admin-olx-scraper-health");
      if (olxHealthBtn) olxHealthBtn.addEventListener("click", adminCheckOlxScraperHealth);
      var olxClickerStartBtn = document.getElementById("admin-olx-clicker-start");
      if (olxClickerStartBtn) olxClickerStartBtn.addEventListener("click", startAdminOlxClickerScraper);
      var olxClickerRefreshStatsBtn = document.getElementById("admin-olx-clicker-refresh-stats");
      if (olxClickerRefreshStatsBtn) olxClickerRefreshStatsBtn.addEventListener("click", loadAdminOlxClickerStats);
      initAdminVastRuntimeSettings();
      initAdminQueueControls();
      initAdminScheduler();
      initAdminFeedback();
    }
  }

  function loadAdminLlmRegions() {
    var listEl = document.getElementById("admin-llm-regions-list");
    var statusEl = document.getElementById("admin-llm-regions-status");
    if (!listEl) return;
    listEl.innerHTML = "<p class=\"admin-hint\">Завантаження...</p>";
    if (statusEl) statusEl.textContent = "";
    fetch("/api/admin/llm-processing-regions", { headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) throw new Error("Помилка завантаження");
        return r.json();
      })
      .then(function (data) {
        var all = data.all_regions || [];
        var enabled = (data.enabled_regions || []).slice();
        if (enabled.length === 0 && all.length > 0) {
          enabled = all.slice();
        }
        listEl.innerHTML = "";
        all.forEach(function (name) {
          var label = document.createElement("label");
          label.style.display = "block";
          label.style.marginBottom = "4px";
          var cb = document.createElement("input");
          cb.type = "checkbox";
          cb.dataset.region = name;
          cb.checked = enabled.indexOf(name) !== -1;
          label.appendChild(cb);
          label.appendChild(document.createTextNode(" " + name));
          listEl.appendChild(label);
        });
      })
      .catch(function (e) {
        listEl.innerHTML = "<p class=\"admin-hint\" style=\"color:var(--error)\">" + (e.message || "Помилка") + "</p>";
      });
  }

  function initAdminLlmRegions() {
    var saveBtn = document.getElementById("admin-llm-regions-save");
    var statusEl = document.getElementById("admin-llm-regions-status");
    if (saveBtn) {
      saveBtn.addEventListener("click", function () {
        var listEl = document.getElementById("admin-llm-regions-list");
        if (!listEl) return;
        var checked = [];
        listEl.querySelectorAll("input[type=checkbox][data-region]").forEach(function (cb) {
          if (cb.checked) checked.push(cb.dataset.region);
        });
        if (statusEl) statusEl.textContent = "Збереження...";
        fetch("/api/admin/llm-processing-regions", {
          method: "PUT",
          headers: apiHeaders(),
          body: JSON.stringify({ enabled_regions: checked })
        })
          .then(function (r) {
            if (!r.ok) return r.json().then(function (b) { throw new Error(b.detail || "Помилка"); });
            return r.json();
          })
          .then(function () {
            if (statusEl) statusEl.textContent = "Збережено.";
            var modal = document.getElementById("admin-llm-backfill-modal");
            if (modal) modal.classList.remove("hidden");
          })
          .catch(function (e) {
            if (statusEl) statusEl.textContent = (e.message || "Помилка");
          });
      });
    }
  }

  function initAdminSubtabs() {
    var tabs = document.querySelectorAll("#screen-admin .admin-subtab");
    var blocks = document.querySelectorAll("#screen-admin .admin-block[data-admin-tab]");
    if (!tabs.length || !blocks.length) return;
    function onTabActivated(tabName) {
      if (tabName === "overview") {
        loadAdminUsageStats();
      }
      if (tabName === "queues") {
        loadAdminQueueStatus();
      }
      if (tabName === "cadastral") {
        loadAdminCadastralStats();
      }
      if (tabName === "data") {
        loadAdminLlmRegions();
      }
      if (tabName === "config") {
        loadAdminIntegrityStatus();
      }
    }
    function applyTab(tabName) {
      blocks.forEach(function (block) {
        var t = block.getAttribute("data-admin-tab");
        if (t === tabName) {
          block.classList.remove("hidden");
        } else {
          block.classList.add("hidden");
        }
      });
    }
    tabs.forEach(function (btn) {
      btn.addEventListener("click", function () {
        var tab = btn.getAttribute("data-tab");
        if (!tab) return;
        tabs.forEach(function (b) {
          b.classList.toggle("active", b === btn);
          if (b === btn) {
            b.setAttribute("aria-selected", "true");
          } else {
            b.setAttribute("aria-selected", "false");
          }
        });
        applyTab(tab);
        onTabActivated(tab);
      });
    });
    var active = document.querySelector("#screen-admin .admin-subtab.active");
    var initialTab = (active && active.getAttribute("data-tab")) || "overview";
    applyTab(initialTab);
    onTabActivated(initialTab);
  }

  function initAdminQueueControls() {
    var refreshBtn = document.getElementById("admin-queue-refresh");
    if (refreshBtn) {
      refreshBtn.addEventListener("click", function () {
        loadAdminQueueStatus();
      });
    }
    var cards = document.getElementById("admin-queue-cards");
    if (cards && !cards.dataset.bound) {
      cards.addEventListener("click", function (e) {
        var btn = e.target && e.target.closest("button[data-queue][data-action]");
        if (!btn) return;
        e.preventDefault();
        var queueName = btn.getAttribute("data-queue");
        var action = btn.getAttribute("data-action");
        if (!queueName || !action) return;
        controlAdminQueue(queueName, action);
      });
      cards.dataset.bound = "1";
    }
  }

  function loadAdminQueueStatus() {
    var statusEl = document.getElementById("admin-queue-controls-status");
    var cardsEl = document.getElementById("admin-queue-cards");
    if (!cardsEl) return;
    cardsEl.innerHTML = "<p class=\"admin-hint\">Завантаження стану черг...</p>";
    if (statusEl) statusEl.textContent = "";
    fetch("/api/admin/task-queues/status", { headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) return r.json().then(function (b) { throw new Error(b.detail || "Помилка"); });
        return r.json();
      })
      .then(function (data) {
        renderAdminQueueStatus(data);
      })
      .catch(function (err) {
        cardsEl.innerHTML = "<p class=\"admin-hint\" style=\"color:var(--tg-theme-destructive-text-color)\">" + (err.message || "Не вдалося завантажити стан черг") + "</p>";
      });
  }

  function renderAdminQueueStatus(data) {
    var statusEl = document.getElementById("admin-queue-controls-status");
    var cardsEl = document.getElementById("admin-queue-cards");
    if (!cardsEl) return;
    var queues = (data && data.queues) || [];
    if (!data.task_queue_enabled) {
      if (statusEl) statusEl.textContent = "Brokered queue вимкнено в налаштуваннях (task_queue_enabled=false).";
    } else if (statusEl) {
      statusEl.textContent = "Черга увімкнена. Керування станом застосовується одразу для нових задач.";
    }
    if (!queues.length) {
      cardsEl.innerHTML = "<p class=\"admin-hint\">Немає даних про черги.</p>";
      return;
    }
    cardsEl.innerHTML = queues.map(function (q) {
      var name = q.queue_name || "unknown";
      var state = q.control_state || "running";
      var counts = q.counts || {};
      var rabbit = q.rabbit_messages;
      var total = 0;
      Object.keys(counts).forEach(function (k) { total += Number(counts[k] || 0); });
      var latest = q.latest_task || null;
      var latestText = "—";
      if (latest && latest.task_id) {
        latestText = "Остання: " + (latest.state || "unknown") + " (" + String(latest.task_id).slice(0, 8) + "...)";
      }
      var stateClass = state === "disabled" ? "error" : state === "paused" ? "running" : "done";
      var rabbitText = rabbit == null ? "н/д" : String(rabbit);
      var runningCount = Number(counts.running || 0) + Number(counts.started || 0) + Number(counts.received || 0) + Number(counts.retry || 0);
      return (
        "<div class=\"admin-queue-card\">" +
          "<div class=\"admin-queue-card-header\">" +
            "<div><strong>" + escapeHtml(name) + "</strong></div>" +
            "<span class=\"admin-queue-state " + stateClass + "\">" + escapeHtml(state) + "</span>" +
          "</div>" +
          "<div class=\"admin-queue-metrics\">" +
            "<span>RabbitMQ: " + escapeHtml(rabbitText) + "</span>" +
            "<span>В БД: " + escapeHtml(String(total)) + "</span>" +
            "<span>Активних: " + escapeHtml(String(runningCount)) + "</span>" +
          "</div>" +
          "<div class=\"admin-hint\">" + escapeHtml(latestText) + "</div>" +
          "<div class=\"admin-queue-actions\">" +
            "<button type=\"button\" class=\"btn btn-small\" data-queue=\"" + escapeHtml(name) + "\" data-action=\"resume\">Resume</button>" +
            "<button type=\"button\" class=\"btn btn-small\" data-queue=\"" + escapeHtml(name) + "\" data-action=\"pause\">Pause</button>" +
            "<button type=\"button\" class=\"btn btn-danger btn-small\" data-queue=\"" + escapeHtml(name) + "\" data-action=\"disable\">Disable</button>" +
          "</div>" +
        "</div>"
      );
    }).join("");
  }

  function controlAdminQueue(queueName, action) {
    var statusEl = document.getElementById("admin-queue-controls-status");
    if (statusEl) statusEl.textContent = "Оновлення стану черги " + queueName + "...";
    fetch("/api/admin/task-queues/control", {
      method: "POST",
      headers: apiHeaders(),
      body: JSON.stringify({ queue_name: queueName, action: action })
    })
      .then(function (r) {
        if (!r.ok) return r.json().then(function (b) { throw new Error(b.detail || "Помилка"); });
        return r.json();
      })
      .then(function () {
        if (statusEl) statusEl.textContent = "Стан черги " + queueName + " оновлено (" + action + ").";
        loadAdminQueueStatus();
      })
      .catch(function (err) {
        if (statusEl) statusEl.textContent = "Помилка: " + (err.message || "Не вдалося оновити стан черги");
      });
  }

  function adminCheckOlxScraperHealth() {
    var statusEl = document.getElementById("admin-olx-scraper-health-status");
    if (!statusEl) return;
    statusEl.className = "admin-data-update-status running";
    statusEl.classList.remove("hidden");
    statusEl.textContent = "Перевірка стану скрапера...";
    fetch("/api/admin/olx-scraper/health", { headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) return r.json().then(function (b) { throw new Error(b.detail || "Помилка"); });
        return r.json();
      })
      .then(function (data) {
        var msg = "Оголошень у вибірці: " + data.total +
          ", описів: " + data.with_description + " (" + (data.ratio_description * 100).toFixed(1) + "%)" +
          ", параметрів: " + data.with_parameters + " (" + (data.ratio_parameters * 100).toFixed(1) + "%). ";
        if (data.warning) {
          msg += "Попередження: " + (data.message || "можливі зміни структури OLX.");
          if (data.sample_problem_urls && data.sample_problem_urls.length) {
            msg += " Приклади URL без опису: " + data.sample_problem_urls.join(", ");
          }
          statusEl.className = "admin-data-update-status error";
        } else {
          msg += data.message || "Парсер виглядає здоровим.";
          statusEl.className = "admin-data-update-status done";
        }
        statusEl.textContent = msg;
      })
      .catch(function (e) {
        statusEl.className = "admin-data-update-status error";
        statusEl.textContent = e.message || "Помилка перевірки скрапера.";
      });
  }

  // Експортуємо в глобальний скоуп на випадок, якщо обробники подій викликають глобальну функцію
  if (typeof window !== "undefined") {
    window.adminCheckOlxScraperHealth = adminCheckOlxScraperHealth;
  }
    var modal = document.getElementById("admin-llm-backfill-modal");
    var backfillRun = document.getElementById("admin-llm-backfill-run");
    var backfillSkip = document.getElementById("admin-llm-backfill-skip");
    if (backfillSkip && modal) {
      backfillSkip.addEventListener("click", function () { modal.classList.add("hidden"); });
    }
    if (backfillRun && modal) {
      backfillRun.addEventListener("click", function () {
        var daysSel = document.getElementById("admin-llm-backfill-days");
        var inactiveCb = document.getElementById("admin-llm-backfill-inactive");
        var days = daysSel ? parseInt(daysSel.value, 10) : 7;
        var processInactive = inactiveCb ? !!inactiveCb.checked : false;
        modal.classList.add("hidden");
        if (statusEl) statusEl.textContent = "Оновлення запущено...";
        fetch("/api/admin/llm-processing-regions/backfill", {
          method: "POST",
          headers: apiHeaders(),
          body: JSON.stringify({ days: days, process_inactive: processInactive })
        })
          .then(function (r) {
            if (!r.ok) return r.json().then(function (b) { throw new Error(b.detail || "Помилка"); });
            return r.json();
          })
          .then(function (data) {
            var taskId = data.task_id;
            if (taskId && statusEl) {
              var poll = function () {
                fetch("/api/admin/data-update/status?task_id=" + encodeURIComponent(taskId), { headers: apiHeaders() })
                  .then(function (r) { return r.json(); })
                  .then(function (t) {
                    if (t.status === "done" || t.status === "error") {
                      statusEl.textContent = t.message || t.status;
                      return;
                    }
                    setTimeout(poll, 2000);
                  });
              };
              setTimeout(poll, 2000);
            }
          })
          .catch(function (e) {
            if (statusEl) statusEl.textContent = (e.message || "Помилка запуску");
          });
      });
    }
  }

  function initAdminFeedback() {
    var toggle = document.getElementById("admin-feedback-toggle");
    var content = document.getElementById("admin-feedback-content");
    if (toggle && content) {
      toggle.addEventListener("click", function () {
        var collapsed = content.classList.toggle("collapsed");
        toggle.textContent = collapsed ? "▼" : "▲";
        toggle.setAttribute("aria-expanded", !collapsed);
        if (!collapsed) loadAdminFeedback();
      });
    }
  }

  function loadAdminFeedback() {
    var container = document.getElementById("admin-feedback-list");
    if (!container) return;
    container.innerHTML = "<p class=\"admin-hint\">Завантаження...</p>";
    fetch("/api/admin/feedback/dislikes?limit=50&days=14", { headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) throw new Error("Помилка завантаження");
        return r.json();
      })
      .then(function (data) {
        var items = data.items || [];
        container.innerHTML = "";
        if (items.length === 0) {
          container.innerHTML = "<p class=\"admin-hint\">Немає дизлайків за останні 14 днів.</p>";
          return;
        }
        items.forEach(function (fb) {
          var wrap = document.createElement("div");
          wrap.className = "admin-feedback-item";
          var created = fb.created_at ? new Date(fb.created_at).toLocaleString("uk-UA") : "";
          var convCount = (fb.conversation || []).length;
          var summary = "user_id=" + (fb.user_id || "") + ", " + created;
          if (convCount) summary += ", " + convCount + " повідомлень";
          var header = document.createElement("div");
          header.className = "admin-feedback-item-header";
          header.innerHTML = "<strong>" + (fb.user_query || "").slice(0, 60) + (fb.user_query && fb.user_query.length > 60 ? "…" : "") + "</strong><br><span class=\"admin-feedback-meta\">" + summary + "</span>";
          header.onclick = function () {
            var body = wrap.querySelector(".admin-feedback-item-body");
            if (body) body.classList.toggle("hidden");
          };
          wrap.appendChild(header);
          var body = document.createElement("div");
          body.className = "admin-feedback-item-body hidden";
          var parts = [];
          if (fb.user_query) parts.push("<p><strong>Запит:</strong> " + escapeHtml(fb.user_query) + "</p>");
          if (fb.response_text) parts.push("<p><strong>Відповідь:</strong><pre>" + escapeHtml((fb.response_text || "").slice(0, 3000)) + (fb.response_text && fb.response_text.length > 3000 ? "…" : "") + "</pre></p>");
          if (fb.diagnostic_result && fb.diagnostic_result.issues && fb.diagnostic_result.issues.length) {
            parts.push("<p><strong>Діагностика:</strong><ul>");
            fb.diagnostic_result.issues.forEach(function (i) { parts.push("<li>" + escapeHtml(i) + "</li>"); });
            parts.push("</ul></p>");
          }
          if (fb.conversation && fb.conversation.length) {
            parts.push("<p><strong>Повна бесіда:</strong></p><div class=\"admin-feedback-conversation\">");
            fb.conversation.forEach(function (m) {
              var role = m.role === "user" ? "Користувач" : "Асистент";
              var txt = (m.content || "").slice(0, 2000);
              if ((m.content || "").length > 2000) txt += "…";
              parts.push("<div class=\"admin-feedback-msg admin-feedback-msg-" + (m.role || "") + "\"><strong>" + escapeHtml(role) + ":</strong><pre>" + escapeHtml(txt) + "</pre></div>");
            });
            parts.push("</div>");
          }
          body.innerHTML = parts.join("");
          wrap.appendChild(body);
          container.appendChild(wrap);
        });
      })
      .catch(function (err) {
        container.innerHTML = "<p class=\"admin-hint\" style=\"color:var(--tg-theme-destructive-text-color)\">" + (err.message || "Помилка") + "</p>";
      });
  }

  function escapeHtml(s) {
    if (!s) return "";
    var div = document.createElement("div");
    div.textContent = s;
    return div.innerHTML;
  }

  function initAdminScheduler() {
    var toggle = document.getElementById("admin-scheduler-toggle");
    var content = document.getElementById("admin-scheduler-content");
    if (toggle && content) {
      toggle.addEventListener("click", function () {
        var collapsed = content.classList.toggle("collapsed");
        toggle.textContent = collapsed ? "▼" : "▲";
        toggle.setAttribute("aria-expanded", !collapsed);
        if (!collapsed) loadSchedulerEvents();
      });
    }
    var addBtn = document.getElementById("admin-scheduler-add");
    if (addBtn) addBtn.addEventListener("click", adminSchedulerAdd);
  }

  function loadSchedulerEvents() {
    var container = document.getElementById("admin-scheduler-events");
    if (!container) return;
    fetch("/api/admin/scheduler/events", { headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) throw new Error("Помилка завантаження");
        return r.json();
      })
      .then(function (data) {
        var events = data.events || [];
        container.innerHTML = "";
        if (events.length === 0) {
          container.innerHTML = "<p class=\"admin-hint\">Немає запланованих оновлень. Додайте нижче.</p>";
          return;
        }
        events.forEach(function (ev) {
          var sourcesLabel = ev.sources === "all" ? "ProZorro+OLX" : ev.sources === "prozorro" ? "ProZorro" : "OLX";
          var item = document.createElement("div");
          item.className = "admin-scheduler-event";
          item.innerHTML = "<div class=\"admin-scheduler-event-info\">Щодня о " + String(ev.hour).padStart(2, "0") + ":" + String(ev.minute).padStart(2, "0") + " — " + ev.days + " дн., " + sourcesLabel + "</div><div class=\"admin-scheduler-event-actions\"><button type=\"button\" class=\"btn btn-danger btn-small\" data-id=\"" + (ev.id || "") + "\">Видалити</button></div>";
          var delBtn = item.querySelector("button[data-id]");
          if (delBtn) delBtn.addEventListener("click", function () { adminSchedulerDelete(ev.id); });
          container.appendChild(item);
        });
      })
      .catch(function (err) {
        container.innerHTML = "<p class=\"admin-hint\" style=\"color:var(--tg-theme-destructive-text-color)\">" + (err.message || "Помилка") + "</p>";
      });
  }

  function adminSchedulerAdd() {
    var hour = parseInt(document.getElementById("scheduler-hour").value, 10);
    var minute = parseInt(document.getElementById("scheduler-minute").value, 10);
    var days = parseInt(document.getElementById("scheduler-days").value, 10);
    var sources = document.getElementById("scheduler-sources").value;
    fetch("/api/admin/scheduler/events", {
      method: "POST",
      headers: apiHeaders(),
      body: JSON.stringify({ hour: hour, minute: minute, days: days, sources: sources })
    })
      .then(function (r) {
        if (!r.ok) return r.json().then(function (d) { throw new Error(d.detail || "Помилка"); });
        return r.json();
      })
      .then(function (data) {
        loadSchedulerEvents();
        alert(data.message || "Додано.");
      })
      .catch(function (err) {
        alert(err.message || "Не вдалося додати");
      });
  }

  function adminSchedulerDelete(eventId) {
    if (!eventId || !confirm("Вимкнути це планове оновлення?")) return;
    fetch("/api/admin/scheduler/events/" + encodeURIComponent(eventId), {
      method: "DELETE",
      headers: apiHeaders()
    })
      .then(function (r) {
        if (!r.ok) return r.json().then(function (d) { throw new Error(d.detail || "Помилка"); });
        return r.json();
      })
      .then(function (data) {
        loadSchedulerEvents();
      })
      .catch(function (err) {
        alert(err.message || "Не вдалося видалити");
      });
  }

  function adminRebuildAnalytics() {
    var statusEl = document.getElementById("admin-analytics-status");
    if (statusEl) statusEl.textContent = "Перерахунок...";
    fetch("/api/analytics/rebuild", { method: "POST", headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) return r.json().then(function (d) { throw new Error(d.detail || "Помилка"); });
        return r.json();
      })
      .then(function (data) {
        if (statusEl) statusEl.textContent = "Готово. Індикатори: " + (data.counts && data.counts.indicators) + ", агрегатів: " + (data.counts && data.counts.aggregates);
      })
      .catch(function (err) {
        if (statusEl) statusEl.textContent = "Помилка: " + (err.message || "Не вдалося");
      });
  }

  function adminProcessAnomalousPrices() {
    var statusEl = document.getElementById("admin-anomalous-prices-status");
    if (statusEl) statusEl.textContent = "Пошук...";
    fetch("/api/admin/process-anomalous-prices?limit=50", { method: "POST", headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) return r.json().then(function (d) { throw new Error(d.detail || "Помилка"); });
        return r.json();
      })
      .then(function (data) {
        var found = data.found || 0;
        var items = data.items || [];
        if (statusEl) statusEl.textContent = "Знайдено: " + found + " оголошень з аномальними цінами.";
        if (items.length > 0 && statusEl) {
          var list = items.slice(0, 5).map(function (i) {
            return (i.title || "").slice(0, 40) + " — " + (i.anomaly_type || "") + " (" + (i.value || "") + ")";
          }).join("; ");
          statusEl.textContent += " Приклади: " + list + (items.length > 5 ? "…" : "");
        }
      })
      .catch(function (err) {
        if (statusEl) statusEl.textContent = "Помилка: " + (err.message || "Не вдалося");
      });
  }

  var adminUsageCharts = { llm: null, geocoding: null, gpuRuntime: null };

  function loadAdminUsageStats() {
    var summaryEl = document.getElementById("admin-usage-stats-summary");
    if (!summaryEl) return;
    summaryEl.textContent = "Завантаження...";
    fetch("/api/admin/usage-stats?days=60", { headers: apiHeaders() })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        var llm = data.llm || {};
        var geo = data.geocoding || {};
        var gpu = data.gpu_runtime || {};
        var inp = llm.input_tokens_total || 0;
        var out = llm.output_tokens_total || 0;
        var cost = llm.estimated_cost_usd != null ? llm.estimated_cost_usd : 0;
        var inpMonth = llm.input_tokens_last_month || 0;
        var outMonth = llm.output_tokens_last_month || 0;
        var costMonth = llm.estimated_cost_usd_last_month != null ? llm.estimated_cost_usd_last_month : 0;
        var llmStr = "LLM: " + inp.toLocaleString() + " вх. / " + out.toLocaleString() + " вих. токенів";
        if (cost > 0) llmStr += ", ~$" + cost.toFixed(4) + " USD";
        llmStr += " (за міс.: " + (inpMonth + outMonth).toLocaleString() + " токенів";
        if (costMonth > 0) llmStr += ", ~$" + costMonth.toFixed(4);
        llmStr += ")";
        if (llm.user_queries_total != null) llmStr += ", запитів користувачів " + llm.user_queries_total;
        if (llm.model) llmStr += " [" + llm.model + "]";
        var geoStr = "Geocoding API: " + (geo.total || 0) + " викликів (за міс. " + (geo.last_month || 0) + ")";
        if (geo.cache_hits_total != null) geoStr += ", з кешу " + geo.cache_hits_total;
        var gpuTotalMin = gpu.active_minutes_total || 0;
        var gpuMonthMin = gpu.active_minutes_last_month || 0;
        var gpuCost = gpu.billed_cost_usd_total != null ? gpu.billed_cost_usd_total : (gpu.estimated_cost_usd_total || 0);
        var gpuCostMonth = gpu.billed_cost_usd_last_month != null ? gpu.billed_cost_usd_last_month : (gpu.estimated_cost_usd_last_month || 0);
        var gpuStr = "GPU runtime: " + gpuTotalMin.toFixed(1) + " хв";
        if (gpuCost > 0) gpuStr += ", Vast $" + gpuCost.toFixed(4);
        gpuStr += " (за міс. " + gpuMonthMin.toFixed(1) + " хв";
        if (gpuCostMonth > 0) gpuStr += ", Vast $" + gpuCostMonth.toFixed(4);
        gpuStr += ")";
        summaryEl.innerHTML = "<strong>" + llmStr + "</strong> &nbsp;|&nbsp; <strong>" + geoStr + "</strong> &nbsp;|&nbsp; <strong>" + gpuStr + "</strong>";
        if (data.error) summaryEl.innerHTML += " <span class=\"admin-hint\">(" + data.error + ")</span>";
        renderAdminUsageCharts(llm.by_day || [], geo.by_day || [], gpu.by_day || []);
      })
      .catch(function (err) {
        summaryEl.textContent = "Помилка: " + (err.message || "невідома");
      });
  }

  function renderAdminUsageCharts(llmByDay, geoByDay, gpuByDay) {
    var llmCanvas = document.getElementById("admin-chart-llm");
    var geoCanvas = document.getElementById("admin-chart-geocoding");
    var gpuCanvas = document.getElementById("admin-chart-gpu-runtime");
    if (!llmCanvas || !geoCanvas || !gpuCanvas || typeof Chart === "undefined") return;

    var chartOpts = {
      responsive: true,
      maintainAspectRatio: true,
      aspectRatio: 2,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { maxRotation: 45, font: { size: 10 } } },
        y: { beginAtZero: true, ticks: { stepSize: 1 } }
      }
    };

    if (adminUsageCharts.llm) adminUsageCharts.llm.destroy();
    adminUsageCharts.llm = new Chart(llmCanvas, {
      type: "bar",
      data: {
        labels: llmByDay.map(function (d) { return d.date; }),
        datasets: [{ label: "Токени LLM (вх+вих)", data: llmByDay.map(function (d) { return d.count != null ? d.count : (d.input_tokens || 0) + (d.output_tokens || 0); }), backgroundColor: "rgba(33, 150, 243, 0.6)", borderColor: "rgb(33, 150, 243)", borderWidth: 1 }]
      },
      options: chartOpts
    });

    if (adminUsageCharts.geocoding) adminUsageCharts.geocoding.destroy();
    adminUsageCharts.geocoding = new Chart(geoCanvas, {
      type: "bar",
      data: {
        labels: geoByDay.map(function (d) { return d.date; }),
        datasets: [{ label: "Geocoding API", data: geoByDay.map(function (d) { return d.count; }), backgroundColor: "rgba(76, 175, 80, 0.6)", borderColor: "rgb(76, 175, 80)", borderWidth: 1 }]
      },
      options: chartOpts
    });

    if (adminUsageCharts.gpuRuntime) adminUsageCharts.gpuRuntime.destroy();
    adminUsageCharts.gpuRuntime = new Chart(gpuCanvas, {
      type: "bar",
      data: {
        labels: gpuByDay.map(function (d) { return d.date; }),
        datasets: [{ label: "GPU runtime (хв)", data: gpuByDay.map(function (d) { return d.active_minutes || 0; }), backgroundColor: "rgba(255, 152, 0, 0.6)", borderColor: "rgb(255, 152, 0)", borderWidth: 1 }]
      },
      options: chartOpts
    });
  }

  function initAdminVastRuntimeSettings() {
    var saveBtn = document.getElementById("admin-vast-settings-save");
    if (!saveBtn) return;
    loadAdminVastRuntimeSettings();
    saveBtn.addEventListener("click", saveAdminVastRuntimeSettings);
  }

  function loadAdminVastRuntimeSettings() {
    var statusEl = document.getElementById("admin-vast-settings-status");
    fetch("/api/admin/vast-runtime-settings", { headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) return r.json().then(function (b) { throw new Error(b.detail || "Помилка"); });
        return r.json();
      })
      .then(function (cfg) {
        var byId = function (id) { return document.getElementById(id); };
        if (byId("admin-vast-enabled")) byId("admin-vast-enabled").checked = !!cfg.is_enabled;
        if (byId("admin-vast-image")) byId("admin-vast-image").value = cfg.image || "";
        if (byId("admin-vast-vllm-model")) byId("admin-vast-vllm-model").value = cfg.vllm_model || "";
        if (byId("admin-vast-vllm-max-len")) byId("admin-vast-vllm-max-len").value = cfg.vllm_max_model_len != null ? cfg.vllm_max_model_len : "";
        if (byId("admin-vast-vllm-gpu-mem")) byId("admin-vast-vllm-gpu-mem").value = cfg.vllm_gpu_memory_utilization != null ? cfg.vllm_gpu_memory_utilization : "";
        if (byId("admin-vast-vllm-max-seqs")) byId("admin-vast-vllm-max-seqs").value = cfg.vllm_max_num_seqs != null ? cfg.vllm_max_num_seqs : "";
        if (byId("admin-vast-vllm-enforce-eager")) byId("admin-vast-vllm-enforce-eager").checked = cfg.vllm_enforce_eager !== false;
        if (byId("admin-vast-min-gpu-ram")) byId("admin-vast-min-gpu-ram").value = cfg.min_gpu_ram_gb != null ? cfg.min_gpu_ram_gb : "";
        if (byId("admin-vast-max-hourly-usd")) byId("admin-vast-max-hourly-usd").value = cfg.max_hourly_usd != null ? cfg.max_hourly_usd : "";
        if (byId("admin-vast-idle-grace-sec")) byId("admin-vast-idle-grace-sec").value = cfg.idle_grace_sec != null ? cfg.idle_grace_sec : "";
        if (byId("admin-vast-endpoint-timeout-sec")) byId("admin-vast-endpoint-timeout-sec").value = cfg.endpoint_timeout_sec != null ? cfg.endpoint_timeout_sec : "";
        if (byId("admin-vast-hard-budget-usd")) byId("admin-vast-hard-budget-usd").value = cfg.hard_budget_usd != null ? cfg.hard_budget_usd : "";
        if (statusEl) statusEl.textContent = "Поточний ключ Vast: " + (cfg.vast_api_key || "не задано") + " | HF token: " + (cfg.hf_token || "не задано");
      })
      .catch(function (e) {
        if (statusEl) statusEl.textContent = "Помилка: " + (e.message || "не вдалося завантажити налаштування");
      });
  }

  function saveAdminVastRuntimeSettings() {
    var statusEl = document.getElementById("admin-vast-settings-status");
    var byId = function (id) { return document.getElementById(id); };
    var payload = {
      is_enabled: !!(byId("admin-vast-enabled") && byId("admin-vast-enabled").checked),
      image: byId("admin-vast-image") ? byId("admin-vast-image").value : "",
      vllm_model: byId("admin-vast-vllm-model") ? byId("admin-vast-vllm-model").value : "",
      vllm_max_model_len: byId("admin-vast-vllm-max-len") ? parseInt(byId("admin-vast-vllm-max-len").value || "4096", 10) : 4096,
      vllm_gpu_memory_utilization: byId("admin-vast-vllm-gpu-mem") ? parseFloat(byId("admin-vast-vllm-gpu-mem").value || "0.9") : 0.9,
      vllm_max_num_seqs: byId("admin-vast-vllm-max-seqs") ? parseInt(byId("admin-vast-vllm-max-seqs").value || "4", 10) : 4,
      vllm_enforce_eager: !!(byId("admin-vast-vllm-enforce-eager") && byId("admin-vast-vllm-enforce-eager").checked),
      min_gpu_ram_gb: byId("admin-vast-min-gpu-ram") ? parseInt(byId("admin-vast-min-gpu-ram").value || "0", 10) : 0,
      max_hourly_usd: byId("admin-vast-max-hourly-usd") ? parseFloat(byId("admin-vast-max-hourly-usd").value || "0") : 0,
      idle_grace_sec: byId("admin-vast-idle-grace-sec") ? parseInt(byId("admin-vast-idle-grace-sec").value || "60", 10) : 60,
      endpoint_timeout_sec: byId("admin-vast-endpoint-timeout-sec") ? parseInt(byId("admin-vast-endpoint-timeout-sec").value || "1200", 10) : 1200,
      hard_budget_usd: byId("admin-vast-hard-budget-usd") ? parseFloat(byId("admin-vast-hard-budget-usd").value || "0") : 0,
    };
    var apiKeyInput = byId("admin-vast-api-key");
    var hfTokenInput = byId("admin-vast-hf-token");
    if (apiKeyInput && apiKeyInput.value && apiKeyInput.value.trim()) {
      payload.vast_api_key = apiKeyInput.value.trim();
    }
    if (hfTokenInput && hfTokenInput.value && hfTokenInput.value.trim()) {
      payload.hf_token = hfTokenInput.value.trim();
    }
    if (statusEl) statusEl.textContent = "Збереження...";
    fetch("/api/admin/vast-runtime-settings", {
      method: "PUT",
      headers: apiHeaders(),
      body: JSON.stringify(payload)
    })
      .then(function (r) {
        if (!r.ok) return r.json().then(function (b) { throw new Error(b.detail || "Помилка"); });
        return r.json();
      })
      .then(function () {
        if (apiKeyInput) apiKeyInput.value = "";
        if (hfTokenInput) hfTokenInput.value = "";
        if (statusEl) statusEl.textContent = "Збережено.";
        loadAdminVastRuntimeSettings();
      })
      .catch(function (e) {
        if (statusEl) statusEl.textContent = "Помилка: " + (e.message || "не вдалося зберегти");
      });
  }

  function loadAdminCadastralStats() {
    var el = document.getElementById("admin-cadastral-stats");
    if (!el) return;
    fetch("/api/admin/cadastral-scraper/stats", { headers: apiHeaders() })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (data.error) {
          el.textContent = "Помилка: " + data.error;
          return;
        }
        var cells = data.cells || {};
        var total = cells.total || 0;
        var done = cells.done || 0;
        var pending = cells.pending || 0;
        var err = cells.error || 0;
        var proc = cells.processing || 0;
        var pct = total ? Math.round(100 * done / total) : 0;
        var dbInfo = data.db_info ? " [" + data.db_info + "]" : "";
        var idxCnt = data.location_index_count != null ? data.location_index_count : 0;
        var clCnt = data.clusters_count != null ? data.clusters_count : 0;
        el.textContent = "Ділянок: " + (data.total_parcels || 0) + dbInfo + " | Тайлів: " + done + "/" + total + " (" + pct + "%) | очікують: " + pending + (err ? ", помилок: " + err : "") + (proc ? ", в роботі: " + proc : "") + " | Індекс: " + idxCnt + ", кластерів: " + clCnt;
      })
      .catch(function () { el.textContent = "Не вдалося завантажити статистику."; });
  }

  function loadAdminOlxClickerStats() {
    var statsEl = document.getElementById("admin-olx-clicker-stats");
    var recentEl = document.getElementById("admin-olx-clicker-recent");
    if (!statsEl) return;
    statsEl.textContent = "Завантаження...";
    fetch("/api/admin/olx-clicker-scraper/stats?limit=30", { headers: apiHeaders() })
      .then(function (r) { return r.ok ? r.json() : Promise.reject(new Error(r.statusText)); })
      .then(function (data) {
        var count = data.count || 0;
        statsEl.textContent = "Збережено клікером у raw_olx_listings: " + count + " записів.";
        if (recentEl) {
          var recent = data.recent || [];
          if (recent.length === 0) {
            recentEl.classList.add("hidden");
            recentEl.innerHTML = "";
          } else {
            recentEl.classList.remove("hidden");
            var html = "<h4 style=\"margin-top: 0.5rem;\">Останні збережені (" + recent.length + ")</h4><ul class=\"admin-olx-clicker-recent-list\">";
            for (var i = 0; i < recent.length; i++) {
              var it = recent[i];
              var title = (it.title && it.title.length > 40) ? it.title.substring(0, 40) + "…" : (it.title || "—");
              var url = it.url || "#";
              html += "<li><a href=\"" + (url.replace(/"/g, "&quot;")) + "\" target=\"_blank\" rel=\"noopener\">" + (title.replace(/</g, "&lt;").replace(/>/g, "&gt;")) + "</a>";
              if (it.loaded_at) html += " <span class=\"admin-hint\">" + it.loaded_at + "</span>";
              html += "</li>";
            }
            html += "</ul>";
            recentEl.innerHTML = html;
          }
        }
      })
      .catch(function () {
        statsEl.textContent = "Не вдалося завантажити результати клікера.";
        if (recentEl) { recentEl.classList.add("hidden"); recentEl.innerHTML = ""; }
      });
  }

  function startAdminOlxClickerScraper() {
    var statusEl = document.getElementById("admin-olx-clicker-status");
    if (!statusEl) return;
    var maxPagesEl = document.getElementById("admin-olx-clicker-max-pages");
    var maxListingsEl = document.getElementById("admin-olx-clicker-max-listings");
    var maxPages = (maxPagesEl && parseInt(maxPagesEl.value, 10)) || 3;
    var maxListings = (maxListingsEl && parseInt(maxListingsEl.value, 10)) || 30;
    maxPages = Math.max(1, Math.min(10, maxPages));
    maxListings = Math.max(5, Math.min(100, maxListings));
    statusEl.classList.remove("hidden");
    statusEl.className = "admin-data-update-status running";
    statusEl.textContent = "Запуск клікера...";
    var url = "/api/admin/olx-clicker-scraper/start?max_pages=" + maxPages + "&max_listings=" + maxListings;
    fetch(url, { method: "POST", headers: apiHeaders() })
      .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, data: d }; }); })
      .then(function (x) {
        if (!x.ok) {
          statusEl.className = "admin-data-update-status error";
          statusEl.textContent = (x.data && x.data.detail) || "Помилка запуску";
          return;
        }
        var taskId = x.data.task_id;
        function poll() {
          fetch("/api/admin/olx-clicker-scraper/status?task_id=" + encodeURIComponent(taskId), { headers: apiHeaders() })
            .then(function (res) { return res.json(); })
            .then(function (st) {
              statusEl.textContent = st.message || st.status;
              if (st.status === "done") {
                statusEl.className = "admin-data-update-status done";
                loadAdminOlxClickerStats();
                return;
              }
              if (st.status === "error") {
                statusEl.className = "admin-data-update-status error";
                loadAdminOlxClickerStats();
                return;
              }
              setTimeout(poll, 2000);
            })
            .catch(function (err) {
              statusEl.className = "admin-data-update-status error";
              statusEl.textContent = "Помилка: " + (err.message || "Не вдалося отримати статус");
            });
        }
        poll();
      })
      .catch(function (err) {
        statusEl.className = "admin-data-update-status error";
        statusEl.textContent = "Помилка: " + (err.message || "Не вдалося запустити");
      });
  }

  function startAdminCadastralScraper(maxCells) {
    var statusEl = document.getElementById("admin-cadastral-status");
    if (!statusEl) return;
    statusEl.classList.remove("hidden");
    statusEl.className = "admin-data-update-status running";
    statusEl.textContent = "Запуск скрапера...";
    var url = "/api/admin/cadastral-scraper/start";
    if (maxCells != null) url += "?max_cells=" + maxCells;
    fetch(url, { method: "POST", headers: apiHeaders() })
      .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, data: d }; }); })
      .then(function (x) {
        if (!x.ok) {
          statusEl.className = "admin-data-update-status error";
          statusEl.textContent = (x.data && x.data.detail) || "Помилка запуску";
          return;
        }
        var taskId = x.data.task_id;
        function poll() {
          fetch("/api/admin/cadastral-scraper/status?task_id=" + encodeURIComponent(taskId), { headers: apiHeaders() })
            .then(function (res) { return res.json(); })
            .then(function (st) {
              statusEl.textContent = st.message || st.status;
              if (st.status === "done") {
                statusEl.className = "admin-data-update-status done";
                loadAdminCadastralStats();
                return;
              }
              if (st.status === "error") {
                statusEl.className = "admin-data-update-status error";
                loadAdminCadastralStats();
                return;
              }
              setTimeout(poll, 2000);
            })
            .catch(function (err) {
              statusEl.className = "admin-data-update-status error";
              statusEl.textContent = "Помилка: " + (err.message || "Не вдалося отримати статус");
            });
        }
        poll();
      })
      .catch(function (err) {
        statusEl.className = "admin-data-update-status error";
        statusEl.textContent = "Помилка: " + (err.message || "Не вдалося запустити");
      });
  }

  function adminCadastralResetStale() {
    fetch("/api/admin/cadastral-scraper/reset-stale", { method: "POST", headers: apiHeaders() })
      .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, data: d }; }); })
      .then(function (x) {
        if (x.ok && x.data.success) {
          loadAdminCadastralStats();
          alert("Скинуто завислих комірок: " + (x.data.reset_count || 0));
        } else {
          alert("Помилка: " + ((x.data && x.data.detail) || "Невідома"));
        }
      })
      .catch(function (err) { alert("Помилка: " + (err.message || "Не вдалося")); });
  }

  function adminCadastralResetCells() {
    if (!confirm("Очистити всі комірки? При наступному запуску буде створена нова сітка (zoom 12, center-first).")) return;
    fetch("/api/admin/cadastral-scraper/reset-cells", { method: "POST", headers: apiHeaders() })
      .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, data: d }; }); })
      .then(function (x) {
        if (x.ok && x.data.success) {
          loadAdminCadastralStats();
          alert("Видалено комірок: " + (x.data.deleted_cells || 0));
        } else {
          alert("Помилка: " + ((x.data && x.data.detail) || "Невідома"));
        }
      })
      .catch(function (err) { alert("Помилка: " + (err.message || "Не вдалося")); });
  }

  function startAdminCadastralBuildIndex() {
    var statusEl = document.getElementById("admin-cadastral-index-status");
    if (!statusEl) return;
    statusEl.classList.remove("hidden");
    statusEl.className = "admin-data-update-status running";
    statusEl.textContent = "Запуск індексації місцезнаходження...";
    fetch("/api/admin/cadastral/index/build", { method: "POST", headers: apiHeaders() })
      .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, data: d }; }); })
      .then(function (x) {
        if (!x.ok) {
          statusEl.className = "admin-data-update-status error";
          statusEl.textContent = (x.data && x.data.detail) || "Помилка запуску";
          return;
        }
        var taskId = x.data.task_id;
        function poll() {
          fetch("/api/admin/cadastral/index/status?task_id=" + encodeURIComponent(taskId), { headers: apiHeaders() })
            .then(function (res) { return res.json(); })
            .then(function (st) {
              statusEl.textContent = st.message || st.status;
              if (st.status === "done") {
                statusEl.className = "admin-data-update-status done";
                loadAdminCadastralStats();
                return;
              }
              if (st.status === "error") {
                statusEl.className = "admin-data-update-status error";
                loadAdminCadastralStats();
                return;
              }
              setTimeout(poll, 3000);
            })
            .catch(function (err) {
              statusEl.className = "admin-data-update-status error";
              statusEl.textContent = "Помилка: " + (err.message || "Не вдалося отримати статус");
            });
        }
        poll();
      })
      .catch(function (err) {
        statusEl.className = "admin-data-update-status error";
        statusEl.textContent = "Помилка: " + (err.message || "Не вдалося запустити");
      });
  }

  function startAdminCadastralBuildClusters() {
    var statusEl = document.getElementById("admin-cadastral-clusters-status");
    if (!statusEl) return;
    statusEl.classList.remove("hidden");
    statusEl.className = "admin-data-update-status running";
    statusEl.textContent = "Запуск кластеризації ділянок...";
    fetch("/api/admin/cadastral/clusters/build", { method: "POST", headers: apiHeaders() })
      .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, data: d }; }); })
      .then(function (x) {
        if (!x.ok) {
          statusEl.className = "admin-data-update-status error";
          statusEl.textContent = (x.data && x.data.detail) || "Помилка запуску";
          return;
        }
        var taskId = x.data.task_id;
        function poll() {
          fetch("/api/admin/cadastral/clusters/status?task_id=" + encodeURIComponent(taskId), { headers: apiHeaders() })
            .then(function (res) { return res.json(); })
            .then(function (st) {
              statusEl.textContent = st.message || st.status;
              if (st.status === "done") {
                statusEl.className = "admin-data-update-status done";
                loadAdminCadastralStats();
                return;
              }
              if (st.status === "error") {
                statusEl.className = "admin-data-update-status error";
                loadAdminCadastralStats();
                return;
              }
              setTimeout(poll, 3000);
            })
            .catch(function (err) {
              statusEl.className = "admin-data-update-status error";
              statusEl.textContent = "Помилка: " + (err.message || "Не вдалося отримати статус");
            });
        }
        poll();
      })
      .catch(function (err) {
        statusEl.className = "admin-data-update-status error";
        statusEl.textContent = "Помилка: " + (err.message || "Не вдалося запустити");
      });
  }

  function adminCadastralClearClusters() {
    if (!confirm("Очистити всі кластери ділянок?")) return;
    fetch("/api/admin/cadastral/clusters/clear", { method: "POST", headers: apiHeaders() })
      .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, data: d }; }); })
      .then(function (x) {
        if (x.ok && x.data.success) {
          loadAdminCadastralStats();
          alert("Очищено кластерів: " + (x.data.deleted || 0));
        } else {
          alert("Помилка: " + ((x.data && x.data.detail) || "Невідома"));
        }
      })
      .catch(function (err) { alert("Помилка: " + (err.message || "Не вдалося")); });
  }

  var adminTargetedUpdateOptionsLoaded = false;

  function loadAdminTargetedUpdateOptions(cb) {
    if (adminTargetedUpdateOptionsLoaded && typeof cb === "function") {
      cb();
      return;
    }
    var regionsEl = document.getElementById("admin-targeted-regions");
    var typesEl = document.getElementById("admin-targeted-listing-types");
    fetch("/api/admin/data-update/options", { headers: apiHeaders() })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        if (regionsEl && data.regions && data.regions.length) {
          regionsEl.innerHTML = data.regions.map(function (r) {
            return "<label class=\"admin-targeted-checkbox\"><input type=\"checkbox\" name=\"admin-targeted-region\" value=\"" + (r.replace(/"/g, "&quot;")) + "\"> " + (r.replace(/</g, "&lt;")) + "</label>";
          }).join("");
        } else if (regionsEl) {
          regionsEl.innerHTML = "<span class=\"admin-hint\">Немає областей (перевірте olx_region_slugs).</span>";
        }
        if (typesEl && data.olx_listing_types && data.olx_listing_types.length) {
          typesEl.innerHTML = data.olx_listing_types.map(function (t) {
            return "<label class=\"admin-targeted-checkbox\"><input type=\"checkbox\" name=\"admin-targeted-listing-type\" value=\"" + (t.replace(/"/g, "&quot;")) + "\"> " + (t.replace(/</g, "&lt;")) + "</label>";
          }).join("");
        } else if (typesEl) {
          typesEl.innerHTML = "<span class=\"admin-hint\">Немає типів.</span>";
        }
        adminTargetedUpdateOptionsLoaded = true;
        if (typeof cb === "function") cb();
      })
      .catch(function () {
        if (regionsEl) regionsEl.innerHTML = "<span class=\"admin-hint\">Не вдалося завантажити опції.</span>";
        if (typesEl) typesEl.innerHTML = "<span class=\"admin-hint\">Не вдалося завантажити опції.</span>";
      });
  }

  function initAdminTargetedUpdate() {
    var toggle = document.getElementById("admin-targeted-update-toggle");
    var content = document.getElementById("admin-targeted-update-content");
    if (toggle && content) {
      toggle.addEventListener("click", function () {
        var expanded = toggle.getAttribute("aria-expanded") === "true";
        toggle.setAttribute("aria-expanded", !expanded);
        content.classList.toggle("collapsed", expanded);
        if (!expanded && !adminTargetedUpdateOptionsLoaded) {
          loadAdminTargetedUpdateOptions();
        }
      });
    }
    var btn = document.getElementById("admin-data-update-targeted");
    if (btn) {
      btn.addEventListener("click", function () {
        loadAdminTargetedUpdateOptions(function () {
          var sourceEl = document.getElementById("admin-targeted-source");
          var daysEl = document.getElementById("admin-targeted-days");
          var regionChecks = document.querySelectorAll("input[name=admin-targeted-region]:checked");
          var typeChecks = document.querySelectorAll("input[name=admin-targeted-listing-type]:checked");
          var source = sourceEl && sourceEl.value ? sourceEl.value : "both";
          var days = daysEl && daysEl.value ? parseInt(daysEl.value, 10) : 7;
          var regions = [];
          var i;
          for (i = 0; i < regionChecks.length; i++) {
            if (regionChecks[i].value) regions.push(regionChecks[i].value);
          }
          var listingTypes = [];
          for (i = 0; i < typeChecks.length; i++) {
            if (typeChecks[i].value) listingTypes.push(typeChecks[i].value);
          }
          startAdminDataUpdate({
            source: source,
            days: days,
            regions: regions.length ? regions : null,
            listing_types: listingTypes.length ? listingTypes : null
          });
        });
      });
    }
  }

  function startAdminDataUpdate(opts) {
    var statusEl = document.getElementById("admin-data-update-status");
    if (!statusEl) return;
    statusEl.classList.remove("hidden");
    statusEl.className = "admin-data-update-status running";
    statusEl.textContent = "Запуск оновлення...";
    var params = new URLSearchParams();
    if (opts && opts.days != null) params.set("days", opts.days);
    if (opts && opts.mode) params.set("mode", opts.mode);
    if (opts && opts.source) params.set("source", opts.source);
    if (opts && opts.regions && (Array.isArray(opts.regions) ? opts.regions.length : opts.regions)) {
      params.set("regions", Array.isArray(opts.regions) ? opts.regions.join(",") : String(opts.regions));
    }
    if (opts && opts.listing_types && (Array.isArray(opts.listing_types) ? opts.listing_types.length : opts.listing_types)) {
      params.set("listing_types", Array.isArray(opts.listing_types) ? opts.listing_types.join(",") : String(opts.listing_types));
    }
    var useBrowserOlxEl = document.getElementById("admin-data-update-use-browser-olx");
    if (useBrowserOlxEl && useBrowserOlxEl.checked) params.set("use_browser_olx", "1");
    var olxPhase1ThreadsEl = document.getElementById("admin-data-update-olx-phase1-threads");
    if (olxPhase1ThreadsEl && olxPhase1ThreadsEl.value !== "" && olxPhase1ThreadsEl.value !== null) {
      var v = parseInt(olxPhase1ThreadsEl.value, 10);
      if (!isNaN(v) && v >= 0) params.set("olx_phase1_max_threads", String(v));
    }
    var url = "/api/admin/data-update" + (params.toString() ? "?" + params.toString() : "");
    fetch(url, { method: "POST", headers: apiHeaders() })
      .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, data: d }; }); })
      .then(function (x) {
        if (!x.ok) {
          statusEl.className = "admin-data-update-status error";
          statusEl.textContent = x.data.detail || "Помилка запуску";
          return;
        }
        var taskId = x.data.task_id;
        function renderProgress(st) {
          var progress = st.progress;
          var total = progress && progress.total > 0 ? progress.total : 0;
          var current = progress && typeof progress.current === "number" ? progress.current : 0;
          var msg = st.message || st.status;
          var parts = [];
          if (total > 0 && statusEl) {
            var pct = Math.min(100, Math.round((current / total) * 100));
            parts.push("<div class=\"admin-data-update-progress-bar\"><div class=\"admin-data-update-progress-fill\" style=\"width:" + pct + "%\"></div></div>");
            parts.push("<div class=\"admin-data-update-progress-meta\">" + (typeof escapeHtml === "function" ? escapeHtml(msg || current + " / " + total) : (msg || current + " / " + total)) + "");
            if (st.started_at && current > 0) {
              var started = new Date(st.started_at).getTime();
              var elapsedSec = (Date.now() - started) / 1000;
              if (elapsedSec >= 1) {
                var perMin = (current / (elapsedSec / 60)).toFixed(1);
                parts.push(" &nbsp;|&nbsp; ~" + perMin + " об/хв");
              }
            }
            parts.push("</div>");
            statusEl.innerHTML = parts.join("");
          } else {
            statusEl.textContent = msg;
          }
        }
        function poll() {
          fetch("/api/admin/data-update/status?task_id=" + encodeURIComponent(taskId), { headers: apiHeaders() })
            .then(function (res) { return res.json(); })
            .then(function (st) {
              if (st.status === "done") {
                statusEl.className = "admin-data-update-status done";
                statusEl.textContent = st.message || "Готово.";
                return;
              }
              if (st.status === "error") {
                statusEl.className = "admin-data-update-status error";
                statusEl.textContent = st.message || "Помилка.";
                return;
              }
              statusEl.className = "admin-data-update-status running";
              renderProgress(st);
              setTimeout(poll, 1500);
            })
            .catch(function (err) {
              statusEl.className = "admin-data-update-status error";
              statusEl.textContent = "Помилка: " + (err.message || "Не вдалося отримати статус");
            });
        }
        poll();
      })
      .catch(function (err) {
        statusEl.className = "admin-data-update-status error";
        statusEl.textContent = "Помилка: " + (err.message || "Не вдалося запустити");
      });
  }

  // Пошук: стан та функції (зведена таблиця unified_listings)
  var searchState = {
    filters: {
      source: null,
      dateFilter: null,
      region: null,
      city: null,
      priceOp: null,
      priceValue: null,
      propertyType: null,
      buildingAreaOp: null,
      buildingAreaValue: null,
      landAreaOp: null,
      landAreaValue: null,
      titleContains: null,
      descriptionContains: null
    },
    sortField: "source_updated_at",
    sortOrder: "desc",
    currentPage: 0,
    pageSize: 20,
    filtersCollapsed: false,
    lastDetailSource: null,
    searchScrollTop: undefined
  };

  var filterOptions = {
    regions: [],
    cities: []
  };
  
  var filterLoadingState = {
    regions: false,
    cities: false
  };
  
  // Ініціалізація пошуку (викликається при завантаженні сторінки)
  function initSearch() {
    console.log("Initializing search functionality");
    // Переконаємося, що екран пошуку існує
    var searchScreen = document.getElementById("screen-search");
    if (!searchScreen) {
      console.error("screen-search element not found in DOM");
    } else {
      console.log("screen-search element found");
    }
  }

  function showSearch() {
    console.log("showSearch called (unified)");
    
    // Скидаємо стан завантаження при відкритті пошуку
    filterLoadingState.regions = false;
    filterLoadingState.cities = false;
    filterOptions.regions = [];
    filterOptions.cities = [];
    
    // Спочатку показуємо екран
    show("screen-search");
    
    // Оновлюємо підсумки фільтрів та сортування на вкладці «Результати»
    updateSearchSummaries();
    
    // Переконаємося, що екран видимий
    var searchScreen = document.getElementById("screen-search");
    if (searchScreen) {
      searchScreen.classList.remove("hidden");
      searchScreen.style.display = "block";
      console.log("Search screen should be visible now");
    } else {
      console.error("screen-search element not found!");
      return;
    }
    
    // Невелика затримка для того, щоб DOM оновився
    setTimeout(function() {
      searchState.currentPage = 0;
      performSearch();
    }, 50);
  }

  function loadFilterOptions(regionForCities) {
    var endpoint = "/api/search/unified";
    var regionToUse = regionForCities || searchState.filters.region;
    
    // Завантажуємо області
    if (!filterLoadingState.regions) {
      filterLoadingState.regions = true;
      fetch(endpoint + "/filters/regions", { headers: apiHeaders() })
        .then(function (r) {
          if (!r.ok) {
            console.error("Failed to load regions:", r.status);
            filterOptions.regions = [];
            filterLoadingState.regions = false;
            updateRegionDropdownIfVisible();
            return null;
          }
          return r.json();
        })
        .then(function (data) {
          filterLoadingState.regions = false;
          if (data && data.regions && Array.isArray(data.regions)) {
            filterOptions.regions = data.regions;
            console.log("Loaded regions:", filterOptions.regions.length);
          } else {
            filterOptions.regions = [];
            console.log("No regions data received");
          }
          updateRegionDropdownIfVisible();
        })
        .catch(function (err) {
          console.error("Error loading regions:", err);
          filterOptions.regions = [];
          filterLoadingState.regions = false;
          updateRegionDropdownIfVisible();
        });
    }
    
    // Завантажуємо міста (завжди: з областю або всі)
    if (!filterLoadingState.cities) {
      filterLoadingState.cities = true;
      var citiesUrl = endpoint + "/filters/cities";
      if (regionToUse) {
        citiesUrl += "?region=" + encodeURIComponent(regionToUse);
      }
      fetch(citiesUrl, { headers: apiHeaders() })
        .then(function (r) {
          if (!r.ok) {
            console.error("Failed to load cities:", r.status);
            filterOptions.cities = [];
            filterLoadingState.cities = false;
            updateCityDropdownIfVisible();
            return null;
          }
          return r.json();
        })
        .then(function (data) {
          filterLoadingState.cities = false;
          if (data && data.cities && Array.isArray(data.cities)) {
            filterOptions.cities = data.cities;
            console.log("Loaded cities:", filterOptions.cities.length);
          } else {
            filterOptions.cities = [];
            console.log("No cities data received");
          }
          updateCityDropdownIfVisible();
        })
        .catch(function (err) {
          console.error("Error loading cities:", err);
          filterOptions.cities = [];
          filterLoadingState.cities = false;
          updateCityDropdownIfVisible();
        });
    }
  }
  
  function updateRegionDropdownIfVisible() {
    var regionInput = document.getElementById("filter-region");
    var dropdown = document.getElementById("filter-region-dropdown");
    if (regionInput && dropdown && !dropdown.classList.contains("hidden")) {
      renderFilterDropdown("region", filterOptions.regions, regionInput.value);
    }
  }
  
  function updateCityDropdownIfVisible() {
    var cityInput = document.getElementById("filter-city");
    var dropdown = document.getElementById("filter-city-dropdown");
    if (cityInput && dropdown && !dropdown.classList.contains("hidden")) {
      renderFilterDropdown("city", filterOptions.cities, cityInput.value);
    }
  }

  function renderConstructorDropdown(inputId, dropdownId, options, searchTerm) {
    var dropdown = document.getElementById(dropdownId);
    var input = document.getElementById(inputId);
    if (!dropdown || !input) return;
    var opts = options || [];
    if (opts.length === 0) {
      dropdown.innerHTML = "<div class='filter-dropdown-item'>Немає даних</div>";
      dropdown.classList.remove("hidden");
      return;
    }
    var filtered = opts.filter(function (o) {
      return !searchTerm || String(o).toLowerCase().indexOf(searchTerm.toLowerCase()) !== -1;
    });
    dropdown.innerHTML = "";
    filtered.forEach(function (opt) {
      var item = document.createElement("div");
      item.className = "filter-dropdown-item";
      item.textContent = String(opt);
      item.dataset.value = String(opt);
      dropdown.appendChild(item);
    });
    dropdown.classList.remove("hidden");
  }

  function renderFilterDropdown(type, options, searchTerm) {
    var dropdown = document.getElementById("filter-" + type + "-dropdown");
    if (!dropdown) {
      console.error("Dropdown not found for type:", type);
      return;
    }
    
    // Перевіряємо чи дані завантажені
    var expectedOptions = type === "region" ? filterOptions.regions : filterOptions.cities;
    
    // Для міста без обраної області — завантажуємо всі міста (включно з Києвом, Севастополем)
    if (type === "city" && !searchState.filters.region && (!filterOptions.cities || filterOptions.cities.length === 0) && !filterLoadingState.cities) {
      loadFilterOptions(null);
    }
    
    // Якщо options не передано або порожній
    if ((!options || options.length === 0) && (!expectedOptions || expectedOptions.length === 0)) {
      var isLoading = type === "region" ? filterLoadingState.regions : filterLoadingState.cities;
      
      if (isLoading) {
        // Дані завантажуються
        dropdown.innerHTML = "<div class='filter-dropdown-item'>Завантаження...</div>";
        dropdown.classList.remove("hidden");
        return;
      } else {
        // Дані завантажені, але порожні - показуємо "Немає даних"
        dropdown.innerHTML = "<div class='filter-dropdown-item'>Немає даних</div>";
        dropdown.classList.remove("hidden");
        return;
      }
    }
    
    // Використовуємо передані options або дані з filterOptions
    var optionsToUse = options && options.length > 0 ? options : expectedOptions;
    
    if (!optionsToUse || optionsToUse.length === 0) {
      dropdown.innerHTML = "<div class='filter-dropdown-item'>Немає даних</div>";
      dropdown.classList.remove("hidden");
      return;
    }
    
    var filtered = optionsToUse.filter(function (opt) {
      if (!opt) return false;
      return !searchTerm || String(opt).toLowerCase().indexOf(searchTerm.toLowerCase()) !== -1;
    });
    
    dropdown.innerHTML = "";
    if (filtered.length === 0) {
      dropdown.innerHTML = "<div class='filter-dropdown-item'>Нічого не знайдено</div>";
    } else {
      filtered.forEach(function (opt) {
        var item = document.createElement("div");
        item.className = "filter-dropdown-item";
        item.textContent = String(opt);
        item.addEventListener("click", function (e) {
          e.stopPropagation();
          var input = document.getElementById("filter-" + type);
          if (input) {
            input.value = String(opt);
            searchState.filters[type] = String(opt);
          }
          dropdown.classList.add("hidden");
          if (type === "region") {
            searchState.filters.city = null;
            var cityInput = document.getElementById("filter-city");
            if (cityInput) cityInput.value = "";
            // Скидаємо стан завантаження міст і завантажуємо їх знову для нової області
            filterLoadingState.cities = false;
            filterOptions.cities = [];
            loadFilterOptions();
          }
        });
        dropdown.appendChild(item);
      });
    }
    dropdown.classList.remove("hidden");
  }

  function performSearch() {
    var loadingEl = document.getElementById("search-loading");
    var itemsEl = document.getElementById("search-items");
    var paginationEl = document.getElementById("search-pagination");
    
    if (!itemsEl) {
      console.error("search-items element not found");
      return;
    }
    
    if (loadingEl) loadingEl.classList.remove("hidden");
    if (itemsEl) itemsEl.innerHTML = "";
    if (paginationEl) paginationEl.innerHTML = "";
    var countEl = document.getElementById("search-results-count");
    if (countEl) countEl.classList.add("hidden");
    
    var endpoint = "/api/search/query";
    var filterStringEl = document.getElementById("filter-string");
    var filterString = (filterStringEl && filterStringEl.value) ? filterStringEl.value.trim() : "";

    fetch(endpoint, {
      method: "POST",
      headers: Object.assign({ "Content-Type": "application/json" }, apiHeaders()),
      body: JSON.stringify({
        filter_string: filterString || "",
        sort_field: searchState.sortField,
        sort_order: searchState.sortOrder,
        limit: searchState.pageSize,
        skip: searchState.currentPage * searchState.pageSize
      })
    })
      .then(function (r) {
        console.log("Response status:", r.status);
        if (!r.ok) {
          return r.json().then(function(errData) {
            throw new Error("Помилка " + r.status + ": " + (errData.detail || r.statusText));
          }).catch(function() {
            throw new Error("Помилка " + r.status);
          });
        }
        return r.json();
      })
      .then(function (data) {
        console.log("Received data:", data);
        if (loadingEl) loadingEl.classList.add("hidden");
        renderSearchResults(data.items || [], data.total || 0);
      })
      .catch(function (err) {
        console.error("Search error:", err);
        if (loadingEl) loadingEl.classList.add("hidden");
        if (itemsEl) itemsEl.innerHTML = "<div class='error'>Помилка: " + (err.message || "Не вдалося завантажити") + "</div>";
        var countEl = document.getElementById("search-results-count");
        if (countEl) countEl.classList.add("hidden");
        var filterStringErr = document.getElementById("filter-string-error");
        if (filterStringErr) {
          filterStringErr.textContent = err.message || "Помилка запиту або парсингу рядка фільтрів";
          filterStringErr.classList.remove("hidden");
        }
      });
  }

  function formatDate(dateStr) {
    if (!dateStr) return "";
    try {
      var date = new Date(dateStr);
      if (isNaN(date.getTime())) return dateStr;
      return date.toLocaleDateString("uk-UA", {
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit"
      });
    } catch (e) {
      return dateStr;
    }
  }

  function pluralizeListings(n) {
    var mod10 = n % 10, mod100 = n % 100;
    if (mod10 === 1 && mod100 !== 11) return "оголошення";
    if (mod10 >= 2 && mod10 <= 4 && (mod100 < 10 || mod100 >= 20)) return "оголошення";
    return "оголошень";
  }

  function fixTitlePriceDisplay(rawTitle, priceUsd) {
    if (!rawTitle || priceUsd == null) return rawTitle || "";
    if (priceUsd < 1000) return rawTitle;
    var bad = rawTitle.match(/\s*\|\s*\$\s*2\s*$/);
    if (bad) return rawTitle.replace(/\s*\|\s*\$\s*2\s*$/, " | " + formatPrice(priceUsd) + " $");
    return rawTitle;
  }

  function renderSearchResults(items, total) {
    var itemsEl = document.getElementById("search-items");
    var paginationEl = document.getElementById("search-pagination");
    var countEl = document.getElementById("search-results-count");
    
    if (!itemsEl) return;
    
    if (countEl) {
      var countText = "Знайдено " + total + " " + pluralizeListings(total);
      if (total > 0 && total > searchState.pageSize) {
        var from = searchState.currentPage * searchState.pageSize + 1;
        var to = Math.min((searchState.currentPage + 1) * searchState.pageSize, total);
        countText += " (показано " + from + "–" + to + ")";
      }
      countEl.textContent = countText;
      countEl.classList.remove("hidden");
    }
    
    if (items.length === 0) {
      itemsEl.innerHTML = "<div class='no-results'>Нічого не знайдено</div>";
      if (paginationEl) paginationEl.innerHTML = "";
      return;
    }
    
    itemsEl.innerHTML = "";
    items.forEach(function (item) {
      var card = document.createElement("div");
      card.className = "search-item-card";
      
      var headerRow = document.createElement("div");
      headerRow.className = "search-item-header-row";
      var title = document.createElement("div");
      title.className = "search-item-title";
      title.textContent = fixTitlePriceDisplay(item.title || "", item.price_usd) || "Без назви";
      headerRow.appendChild(title);
      if (item.source) {
        var sourceBadge = document.createElement("span");
        sourceBadge.className = "search-item-source-badge search-item-source-" + item.source;
        sourceBadge.textContent = item.source === "olx" ? "OLX" : "ProZorro";
        headerRow.appendChild(sourceBadge);
      }
      card.appendChild(headerRow);
      
      var details = document.createElement("div");
      details.className = "search-item-details";
      
      var badgeToShow = (SHOW_ANALYTICS_UI && item.price_indicator) ? item.price_indicator : (item.price_notes || null);
      var badgeSource = item.price_indicator_source || null;
      var mainMetric = (item.price_per_m2_uah && item.price_per_m2_uah > 0) ? "m2" : (item.price_per_ha_uah && item.price_per_ha_uah > 0) ? "ha" : "price";
      var indLabels = { "вигідна": "✓ Вигідна", "нижче середньої": "↓ Нижче середньої", "середня": "— Середня", "вище середньої": "↑ Вище середньої", "дорога": "↑ Дорога", "аномально низька": "⚠ Аномально низька", "аномально висока": "⚠ Аномально висока" };
      function formatBadgeText(ind, src) {
        var base = indLabels[ind] || ind;
        if (src === "city") return base + " (місто)";
        if (src === "region") return base + " (область)";
        return base;
      }
      function addPriceRow(container, label, text, isMain) {
        var wrap = document.createElement("div");
        wrap.className = "search-item-price-row";
        var priceEl = document.createElement("span");
        priceEl.className = "search-item-price";
        priceEl.textContent = label + text;
        wrap.appendChild(priceEl);
        if (badgeToShow && isMain) {
          var badge = document.createElement("span");
          var isIndicator = indLabels.hasOwnProperty(badgeToShow);
          var indicatorClass = (badgeToShow || "").replace(/\s+/g, "-");
          badge.className = "search-item-price-indicator search-item-price-" + (isIndicator ? indicatorClass : "anomalous");
          badge.textContent = formatBadgeText(badgeToShow, badgeSource);
          wrap.appendChild(badge);
        }
        container.appendChild(wrap);
      }

      if (item.price !== null && item.price !== undefined) {
        var baseText = item.price_text || formatPrice(item.price) + " ₴";
        if (item.price_usd !== null && item.price_usd !== undefined) {
          baseText += " (~" + formatPrice(item.price_usd) + " $)";
        }
        addPriceRow(details, "Ціна: ", baseText, mainMetric === "price");
      }

      if (item.price_per_m2_uah !== null && item.price_per_m2_uah !== undefined) {
        var textPpm2 = formatPrice(item.price_per_m2_uah) + " ₴/м²";
        if (item.price_per_m2_usd !== null && item.price_per_m2_usd !== undefined) {
          textPpm2 += " (" + formatPrice(item.price_per_m2_usd) + " $/м²)";
        }
        addPriceRow(details, "Ціна за м²: ", textPpm2, mainMetric === "m2");
      }

      if (item.price_per_ha_uah !== null && item.price_per_ha_uah !== undefined) {
        var pricePerSotka = item.price_per_ha_uah / 100;
        var textPpha = formatPrice(pricePerSotka) + " ₴/с";
        if (item.price_per_ha_usd !== null && item.price_per_ha_usd !== undefined) {
          textPpha += " (" + formatPrice(item.price_per_ha_usd / 100) + " $/с)";
        }
        addPriceRow(details, "Ціна за сотку: ", textPpha, mainMetric === "ha");
      }
      
      var location = [];
      if (item.city) location.push(item.city);
      if (item.region) location.push(item.region);
      if (location.length === 0 && item.location) location.push(item.location);
      
      if (location.length > 0) {
        var loc = document.createElement("div");
        loc.className = "search-item-location";
        loc.textContent = "📍 " + location.join(", ");
        details.appendChild(loc);
      }
      
      var areaVal = item.area_m2 || item.building_area_sqm;
      var landSotky = item.land_area_sotky != null ? item.land_area_sotky : (item.land_area_sqm != null ? item.land_area_sqm / 100 : null);
      if (areaVal) {
        var area = document.createElement("div");
        area.className = "search-item-area";
        area.textContent = "Площа: " + areaVal + " м²";
        details.appendChild(area);
      }
      if (landSotky) {
        var land = document.createElement("div");
        land.className = "search-item-area";
        land.textContent = "Земля: " + (Number(landSotky) === landSotky ? landSotky.toFixed(1) : landSotky) + " с";
        details.appendChild(land);
      }
      if (item.floor !== null && item.floor !== undefined && item.floor !== "") {
        var floorEl = document.createElement("div");
        floorEl.className = "search-item-floor";
        floorEl.textContent = "Поверх: " + item.floor;
        details.appendChild(floorEl);
      }
      if (item.tags && item.tags.length > 0) {
        var tagsWrap = document.createElement("div");
        tagsWrap.className = "search-item-tags";
        var tagsText = item.tags.join(", ");
        tagsWrap.textContent = "Теги: " + tagsText;
        details.appendChild(tagsWrap);
      }
      if (item.status) {
        var status = document.createElement("div");
        status.className = "search-item-status";
        status.textContent = "Статус: " + item.status;
        details.appendChild(status);
      }
      
      // Додаємо дату оновлення
      var dateStr = item.updated_at || item.last_updated || item.date_modified || item.date_created;
      if (dateStr) {
        var dateEl = document.createElement("div");
        dateEl.className = "search-item-date";
        dateEl.textContent = "🕒 Оновлено: " + formatDate(dateStr);
        details.appendChild(dateEl);
      }
      if (item.related_objects_count != null && item.related_objects_count > 0) {
        var relEl = document.createElement("div");
        relEl.className = "search-item-related";
        relEl.textContent = "Пов'язаних об'єктів: " + item.related_objects_count;
        details.appendChild(relEl);
      }
      
      card.appendChild(details);
      
      card.addEventListener("click", function () {
        var detailSource = item.source || "olx";
        var detailId = item.source_id || item.id || item.url || item.auction_id;
        var searchScreen = document.getElementById("screen-search");
        if (searchScreen) searchState.searchScrollTop = searchScreen.scrollTop;
        searchState.lastDetailSource = detailSource;
        showDetail(detailSource, detailId);
      });
      
      itemsEl.appendChild(card);
    });
    
    // Пагінація (20 оголошень на сторінку)
    if (paginationEl) paginationEl.innerHTML = "";
    if (paginationEl && total > searchState.pageSize) {
      var totalPages = Math.ceil(total / searchState.pageSize);
      var pagination = document.createElement("div");
      pagination.className = "pagination";
      
      if (searchState.currentPage > 0) {
        var prev = document.createElement("button");
        prev.className = "btn btn-secondary";
        prev.textContent = "← Попередня";
        prev.addEventListener("click", function () {
          searchState.currentPage--;
          performSearch();
        });
        pagination.appendChild(prev);
      }
      
      var pageInfo = document.createElement("span");
      pageInfo.textContent = "Сторінка " + (searchState.currentPage + 1) + " з " + totalPages + " (всього: " + total + ")";
      pagination.appendChild(pageInfo);
      
      if (searchState.currentPage < totalPages - 1) {
        var next = document.createElement("button");
        next.className = "btn btn-secondary";
        next.textContent = "Наступна →";
        next.addEventListener("click", function () {
          searchState.currentPage++;
          performSearch();
        });
        pagination.appendChild(next);
      }
      
      paginationEl.appendChild(pagination);
    }

    // Відновлення позиції скролу після повернення з картки оголошення (після перемальовування списку)
    if (typeof searchState.searchScrollTop === "number") {
      var scrollEl = document.getElementById("screen-search");
      if (scrollEl) {
        requestAnimationFrame(function () {
          scrollEl.scrollTop = searchState.searchScrollTop;
          searchState.searchScrollTop = undefined;
        });
      }
    }
  }

  function formatPrice(price) {
    if (typeof price !== "number") return String(price || "");
    return new Intl.NumberFormat("uk-UA").format(price);
  }

  // Форматування предметів аукціону ProZorro для міні-апки
  function formatProzorroItem(item, index) {
    var parts = [];

    // Опис / назва предмета
    try {
      var title = null;
      if (item && typeof item === "object") {
        // additionalClassifications[0].description.uk_UA / en_US
        var addCls = item.additionalClassifications;
        if (Array.isArray(addCls) && addCls.length > 0) {
          var ac = addCls[0] || {};
          var desc = ac.description || {};
          if (typeof desc === "string") {
            title = desc;
          } else if (desc.uk_UA || desc.en_US) {
            title = desc.uk_UA || desc.en_US;
          }
        }
        // fallback: item.description.uk_UA / en_US / string
        if (!title && item.description) {
          if (typeof item.description === "string") {
            title = item.description;
          } else if (item.description.uk_UA || item.description.en_US) {
            title = item.description.uk_UA || item.description.en_US;
          }
        }
      }
      if (title) {
        parts.push(title);
      }
    } catch (e) {}

    // Площа / кількість
    try {
      var area = null;
      var areaLabel = null;

      if (item && item.itemProps && typeof item.itemProps === "object") {
        if (typeof item.itemProps.totalBuildingArea === "number") {
          area = item.itemProps.totalBuildingArea;
          areaLabel = "Площа будівлі";
        } else if (typeof item.itemProps.totalObjectArea === "number") {
          area = item.itemProps.totalObjectArea;
          areaLabel = "Площа об'єкта";
        }
      }

      if (area === null && item && item.quantity && typeof item.quantity.value === "number") {
        area = item.quantity.value;
        areaLabel = "Кількість / площа";
      }

      if (area !== null) {
        var unit = null;
        if (item && item.unit) {
          if (typeof item.unit === "string") {
            unit = item.unit;
          } else if (item.unit.code || item.unit.name) {
            unit = item.unit.code || item.unit.name;
          }
        } else if (item && item.quantity && item.quantity.unit) {
          unit = item.quantity.unit;
        }
        var areaText = areaLabel + ": " + area;
        if (unit) {
          areaText += " " + unit;
        }
        parts.push(areaText);
      }
    } catch (e2) {}

    // Адреса
    try {
      if (item && item.address && typeof item.address === "object") {
        var addr = item.address;
        var addrParts = [];
        if (addr.region && addr.region.uk_UA) {
          addrParts.push(addr.region.uk_UA);
        }
        if (addr.locality && addr.locality.uk_UA) {
          addrParts.push(addr.locality.uk_UA);
        }
        if (addr.streetAddress) {
          addrParts.push(addr.streetAddress);
        }
        if (addrParts.length > 0) {
          parts.push("Адреса: " + addrParts.join(", "));
        }
      }
    } catch (e3) {}

    // Класифікатор
    try {
      if (item && item.classification && item.classification.id) {
        parts.push("Код класифікатора: " + item.classification.id);
      }
    } catch (e4) {}

    if (!parts.length) {
      return "Предмет " + (typeof index === "number" ? "#" + (index + 1) : "");
    }
    return parts.join(" · ");
  }

  // Форматування заявок ProZorro
  function formatProzorroBid(bid, index) {
    if (!bid || typeof bid !== "object") {
      return "Заявка " + (typeof index === "number" ? "#" + (index + 1) : "");
    }
    var parts = [];

    var amount = bid.value && typeof bid.value.amount === "number" ? bid.value.amount : null;
    if (amount !== null) {
      parts.push("Сума: " + formatPrice(amount) + " ₴");
    }

    if (bid.status) {
      parts.push("Статус: " + String(bid.status));
    }

    var biddersCount = Array.isArray(bid.bidders) ? bid.bidders.length : 0;
    if (biddersCount) {
      parts.push("Учасників у заявці: " + biddersCount);
    }

    var prefix = "Заявка " + (typeof index === "number" ? "#" + (index + 1) : "");
    if (!parts.length) {
      return prefix;
    }
    return prefix + " — " + parts.join(" · ");
  }

  function getListingExternalUrl(source, sourceId) {
    if (!sourceId) return null;
    var id = sourceId;
    try {
      if (typeof sourceId === "string" && sourceId.indexOf("%") >= 0) {
        id = decodeURIComponent(sourceId);
      }
    } catch (e) {}
    if (source === "olx") return (id + "").indexOf("http") === 0 ? id : "https://www.olx.ua/d/uk/obyavlenie/" + id;
    if (source === "prozorro") return "https://prozorro.sale/auction/" + id;
    return null;
  }

  function openExternalUrl(url) {
    if (!url) return;
    if (Tg && typeof Tg.openLink === "function") {
      Tg.openLink(url);
    } else if (typeof window.open === "function") {
      window.open(url, "_blank", "noopener,noreferrer");
    }
  }

  function openListingInApp(source, sourceId) {
    var detailContent = document.getElementById("detail-content");
    var loadingEl = document.createElement("div");
    loadingEl.textContent = "Завантаження...";
    if (detailContent) detailContent.innerHTML = "";
    if (detailContent) detailContent.appendChild(loadingEl);
    show("screen-detail");
    var endpoint = "/api/search/" + source + "/" + encodeURIComponent(sourceId);
    fetch(endpoint, { headers: apiHeaders() })
      .then(function (r) {
        if (r.status === 404) {
          var extUrl = getListingExternalUrl(source, sourceId);
          openExternalUrl(extUrl);
          if (detailContent) {
            var wrap = document.createElement("div");
            wrap.className = "error detail-404-fallback";
            wrap.appendChild(document.createTextNode("Оголошення не знайдено в базі. "));
            if (extUrl) {
              var a = document.createElement("a");
              a.href = extUrl;
              a.target = "_blank";
              a.rel = "noopener noreferrer";
              a.className = "detail-404-link";
              a.textContent = "Відкрити на " + (source === "olx" ? "OLX" : "ProZorro");
              a.onclick = function (e) { e.preventDefault(); openExternalUrl(extUrl); };
              wrap.appendChild(a);
            } else {
              wrap.appendChild(document.createTextNode("Посилання недоступне."));
            }
            detailContent.innerHTML = "";
            detailContent.appendChild(wrap);
          }
          return null;
        }
        if (!r.ok) throw new Error("Помилка " + r.status);
        return r.json();
      })
      .then(function (data) {
        if (data) renderDetail(data, source);
      })
      .catch(function (err) {
        if (detailContent) {
          detailContent.innerHTML = "<div class='error'>Помилка: " + (err.message || "Не вдалося завантажити") + "</div>";
        }
      });
  }

  function showDetail(type, itemId) {
    openListingInApp(type, itemId);
  }

  function renderUnifiedDetail(item) {
    var detailContent = document.getElementById("detail-content");
    if (!detailContent) return;
    detailContent.innerHTML = "";
    
    var wrap = document.createElement("div");
    wrap.className = "detail-unified";
    
    var title = document.createElement("h3");
    title.className = "detail-unified-title";
    title.textContent = fixTitlePriceDisplay(item.title || "", item.price_usd) || "Без назви";
    wrap.appendChild(title);
    
    var meta = document.createElement("div");
    meta.className = "detail-unified-meta";
    if (item.source) {
      var badge = document.createElement("span");
      badge.className = "search-item-source-badge search-item-source-" + item.source;
      badge.textContent = item.source === "olx" ? "OLX" : "ProZorro";
      meta.appendChild(badge);
    }
    var badgeToShow = (SHOW_ANALYTICS_UI && item.price_indicator) ? item.price_indicator : (item.price_notes || null);
    var badgeSource = item.price_indicator_source || null;
    if (badgeToShow) {
      var badge = document.createElement("span");
      var indLabels = { "вигідна": "✓ Вигідна ціна", "нижче середньої": "↓ Нижче середньої", "середня": "— Середня", "вище середньої": "↑ Вище середньої", "дорога": "↑ Дорога", "аномально низька": "⚠ Аномально низька", "аномально висока": "⚠ Аномально висока" };
      var isIndicator = indLabels.hasOwnProperty(badgeToShow);
      badge.className = "search-item-price-indicator search-item-price-" + (isIndicator ? badgeToShow.replace(/\s+/g, "-") : "anomalous");
      var base = indLabels[badgeToShow] || badgeToShow;
      badge.textContent = badgeSource === "city" ? base + " (місто)" : badgeSource === "region" ? base + " (область)" : base;
      meta.appendChild(badge);
    }
    wrap.appendChild(meta);
    
    function addRow(label, text) {
      if (text === undefined || text === null || text === "") return;
      var row = document.createElement("div");
      row.className = "detail-field";
      var lbl = document.createElement("div");
      lbl.className = "detail-field-label";
      lbl.textContent = label + ":";
      var val = document.createElement("div");
      val.className = "detail-field-value";
      val.textContent = typeof text === "number" ? formatPrice(text) : String(text);
      row.appendChild(lbl);
      row.appendChild(val);
      wrap.appendChild(row);
    }
    
    if (item.price != null) {
      var priceText = formatPrice(item.price) + " ₴";
      if (item.price_usd) priceText += " (~" + formatPrice(item.price_usd) + " $)";
      addRow("Ціна", priceText);
    }
    if (item.price_per_m2_uah != null) addRow("Ціна за м²", formatPrice(item.price_per_m2_uah) + " ₴/м²");
    if (item.price_per_ha_uah != null) addRow("Ціна за сотку", formatPrice(item.price_per_ha_uah / 100) + " ₴/с");
    if (item.region || item.city) addRow("Локація", [item.city, item.region].filter(Boolean).join(", "));
    if (item.building_area_sqm) addRow("Площа", item.building_area_sqm + " м²");
    if (item.land_area_sotky != null || item.land_area_sqm != null) {
      var s = item.land_area_sotky != null ? item.land_area_sotky : (item.land_area_sqm / 100);
      addRow("Земля", (typeof s === "number" ? s.toFixed(1) : s) + " с");
    }
    if (item.status) addRow("Статус", item.status);
    if (item.property_type) addRow("Тип", item.property_type);
    if (item.page_url) {
      var linkRow = document.createElement("div");
      linkRow.className = "detail-field";
      var linkLbl = document.createElement("div");
      linkLbl.className = "detail-field-label";
      linkLbl.textContent = "Посилання:";
      var linkVal = document.createElement("div");
      linkVal.className = "detail-field-value";
      var link = document.createElement("a");
      link.href = item.page_url;
      link.target = "_blank";
      link.className = "detail-link";
      link.textContent = "Відкрити оголошення";
      linkVal.appendChild(link);
      linkRow.appendChild(linkLbl);
      linkRow.appendChild(linkVal);
      wrap.appendChild(linkRow);
    }

    var unifiedParts = [];
    if (item.price != null) unifiedParts.push("ціна " + formatPrice(item.price) + " ₴");
    if (item.region || item.city) unifiedParts.push([item.city, item.region].filter(Boolean).join(", "));
    unifiedParts.push(item.source === "olx" ? "OLX" : "ProZorro");
    var unifiedContext = {
      page_url: item.page_url || "",
      summary: unifiedParts.join(", "),
      detail_source: item.source || "",
      detail_id: item.source_id || item.id || item.url || "",
    };
    var aiBtnWrap = document.createElement("div");
    aiBtnWrap.className = "detail-cta-wrap";
    var aiBtn = document.createElement("button");
    aiBtn.type = "button";
    aiBtn.className = "btn detail-ai-btn";
    aiBtn.textContent = "Спитати у AI-помічника";
    aiBtn.title = "Відкрити чат з AI для аналізу цього оголошення";
    aiBtn.addEventListener("click", function () {
      openChatWithListingContext(unifiedContext);
    });
    aiBtnWrap.appendChild(aiBtn);
    if (currentUser && currentUser.is_admin && item.source && item.source_id) {
      var reformatBtn = document.createElement("button");
      reformatBtn.type = "button";
      reformatBtn.className = "btn detail-reformat-btn";
      reformatBtn.textContent = "Переформатувати дані";
      reformatBtn.title = "Повторна обробка через LLM та геосервіси (тільки для адмінів)";
      reformatBtn.addEventListener("click", function () {
        reformatBtn.disabled = true;
        reformatBtn.textContent = "Обробка...";
        fetch("/api/admin/reformat-listing?source=" + encodeURIComponent(item.source) + "&source_id=" + encodeURIComponent(item.source_id), {
          method: "POST",
          headers: apiHeaders()
        }).then(function (r) { return r.json(); }).then(function (res) {
          reformatBtn.disabled = false;
          reformatBtn.textContent = "Переформатувати дані";
          if (res.success) {
            openListingInApp(item.source, item.source_id);
          } else {
            alert(res.message || "Помилка переформатування");
          }
        }).catch(function (err) {
          reformatBtn.disabled = false;
          reformatBtn.textContent = "Переформатувати дані";
          alert(err.message || "Помилка запиту");
        });
      });
      aiBtnWrap.appendChild(reformatBtn);
    }
    wrap.appendChild(aiBtnWrap);
    
    detailContent.appendChild(wrap);
  }

  // Тимчасово приховано: аналітика рахується, але не показується в UI (недостатньо даних)
  var SHOW_ANALYTICS_UI = true;

  // Детальна локація: показувати лише місто та область; не показувати raw з текстом віджету карти (Google Maps).
  function formatDetailLocation(value) {
    if (value == null) return "";
    if (typeof value === "object") {
      var city = (value.city && String(value.city).trim()) || "";
      var region = (value.region && String(value.region).trim()) || "";
      var parts = [];
      if (city) parts.push("Місто: " + city);
      if (region) parts.push("Область: " + region);
      return parts.length ? parts.join(", ") : "";
    }
    var s = String(value);
    if (s.length > 200 || /перемістити|наблизити|картографічні|©|google|умови|приватність/i.test(s)) return "";
    return s;
  }

  // Конфігурація вкладок та полів для деталей (hero показує назву, ціну, статус, локацію)
  var TAB_FIELD_CONFIG_BASE = {
    prozorro: {
      main: {
        label: "Опис",
        fields: [
          { path: "auction_data.description.uk_UA", label: "Опис", formatter: null, block: true },
          { path: "auction_data.description.en_US", label: "Опис (англ.)", fallback: true, block: true },
          { path: "auction_id", label: "ID аукціону" },
          { path: "auction_data.dateCreated", label: "Дата створення", formatter: function(v) { return formatDate(v); } },
          { path: "auction_data.dateModified", label: "Дата оновлення", formatter: function(v) { return formatDate(v); } },
          { path: "auction_data.enquiryPeriod.endDate", label: "Термін подачі документів", formatter: function(v) { return formatDate(v); }, fallback: true },
          { path: "auction_data.procedureType", label: "Тип процедури" },
          { path: "_detail_link", label: "Посилання", isLink: true, linkKey: "auction_id" }
        ]
      },
      characteristics: {
        label: "Характеристики",
        fields: [
          { path: "auction_data.floor", label: "Поверх", formatter: function(v) { return v ? String(v) : ""; } },
          { path: "auction_data.tags", label: "Теги", formatter: function(v) { return Array.isArray(v) && v.length ? v.join(", ") : ""; } },
          { path: "auction_data.price_metrics.price_per_m2_uah", label: "Ціна за м² (грн)", formatter: function(v) { return v ? formatPrice(v) + " ₴/м²" : ""; } },
          { path: "auction_data.price_metrics.price_per_m2_usd", label: "Ціна за м² (USD)", formatter: function(v) { return v ? formatPrice(v) + " $/м²" : ""; } },
          { path: "auction_data.price_metrics.price_per_ha_uah", label: "Ціна за сотку (грн)", formatter: function(v) { return v ? formatPrice(v / 100) + " ₴/с" : ""; } },
          { path: "auction_data.price_metrics.price_per_ha_usd", label: "Ціна за сотку (USD)", formatter: function(v) { return v ? formatPrice(v / 100) + " $/с" : ""; } },
          { path: "auction_data.value.currency", label: "Валюта" },
          { path: "auction_data.value.valueAddedTaxIncluded", label: "ПДВ включено", formatter: function(v) { return v ? "Так" : "Ні"; } }
        ]
      },
      participants: {
        label: "Учасники",
        fields: [
          { path: "auction_data.minNumberOfQualifiedBids", label: "Мінімальна кількість учасників" },
          { path: "auction_data.bids", label: "Заявки", isArray: true, arrayFormatter: formatProzorroBid }
        ]
      },
      items: {
        label: "Предмети аукціону",
        fields: [
          { path: "auction_data.items", label: "Предмети", isArray: true, arrayFormatter: formatProzorroItem }
        ]
      },
      real_estate: { label: "Об'єкти нерухомості", special: true }
    },
    olx: {
      main: {
        label: "Опис",
        fields: [
          { path: "detail.description", label: "Опис", block: true },
          { path: "detail.contact", label: "Контакти", formatter: function(v) {
            if (!v || typeof v !== "object") return v ? String(v) : "";
            var parts = [];
            if (v.name) parts.push(v.name);
            if (v.phone_preview) parts.push(v.phone_preview);
            if (v.phones && Array.isArray(v.phones)) parts.push(v.phones.join(", "));
            return parts.length ? parts.join(" · ") : JSON.stringify(v);
          } },
          { path: "url", label: "Посилання", isLink: true }
        ]
      },
      characteristics: {
        label: "Характеристики",
        fields: [
          { path: "detail.llm.floor", label: "Поверх", formatter: function(v) { return v ? String(v) : ""; } },
          { path: "search_data.area_m2", label: "Площа", formatter: function(v) { return v ? v + " м²" : ""; } },
          { path: "detail.llm.tags", label: "Теги", formatter: function(v) { return Array.isArray(v) && v.length ? v.join(", ") : ""; }, isArray: false },
          { path: "detail.price_metrics.price_per_m2_uah", label: "Ціна за м² (грн)", formatter: function(v) { return v ? formatPrice(v) + " ₴/м²" : ""; } },
          { path: "detail.price_metrics.price_per_m2_usd", label: "Ціна за м² (USD)", formatter: function(v) { return v ? formatPrice(v) + " $/м²" : ""; } },
          { path: "detail.price_metrics.price_per_ha_uah", label: "Ціна за сотку (грн)", formatter: function(v) { return v ? formatPrice(v / 100) + " ₴/с" : ""; } },
          { path: "detail.price_metrics.price_per_ha_usd", label: "Ціна за сотку (USD)", formatter: function(v) { return v ? formatPrice(v / 100) + " $/с" : ""; } },
          { path: "detail.parameters", label: "Параметри", isArray: true, arrayFormatter: function(item) {
            return formatParameterItem(item);
          }},
          { path: "detail.location", label: "Детальна локація", formatter: formatDetailLocation },
          { path: "search_data.currency", label: "Валюта" }
        ]
      },
      real_estate: { label: "Об'єкти нерухомості", special: true }
    }
  };

  var TAB_FIELD_CONFIG = (function() {
    var cfg = {};
    ["prozorro", "olx"].forEach(function(key) {
      cfg[key] = {};
      for (var k in TAB_FIELD_CONFIG_BASE[key]) {
        cfg[key][k] = TAB_FIELD_CONFIG_BASE[key][k];
      }
    });
    if (SHOW_ANALYTICS_UI) {
      cfg.prozorro.analytics = { label: "Аналітика", special: true };
      cfg.olx.analytics = { label: "Аналітика", special: true };
    }
    return cfg;
  })();

  // Конфігурація полів для відображення (legacy, для сумісності)
  var FIELD_CONFIG = {
    olx: {
      // Службові поля, які не відображаються
      hidden: ["_id", "created_at", "updated_at", "version_hash", "description_hash", "raw_snippet"],
      // Конфігурація полів з пріоритетами (менше число = вище)
      fields: [
        { path: "search_data.title", label: "Назва", priority: 1, description: "Заголовок оголошення" },
        { path: "search_data.price_value", label: "Ціна (грн)", priority: 2, description: "Ціна в гривнях", formatter: function(v) { return formatPrice(v) + " ₴"; } },
        { path: "detail.price_metrics.total_price_usd", label: "Ціна (USD)", priority: 3, description: "Загальна ціна в доларах США", formatter: function(v) { return v ? formatPrice(v) + " $" : ""; } },
        { path: "search_data.price_text", label: "Ціна (текст)", priority: 4, description: "Ціна як вказано в оголошенні" },
        { path: "search_data.location", label: "Розташування", priority: 4, description: "Місто та область" },
        { path: "search_data.area_m2", label: "Площа", priority: 5, description: "Площа в квадратних метрах", formatter: function(v) { return v + " м²"; } },
        { path: "detail.price_metrics.price_per_m2_uah", label: "Ціна за м² (грн)", priority: 6, description: "Ціна за квадратний метр у гривнях", formatter: function(v) { return v ? formatPrice(v) + " ₴/м²" : ""; } },
        { path: "detail.price_metrics.price_per_m2_usd", label: "Ціна за м² (USD)", priority: 7, description: "Ціна за квадратний метр у доларах США", formatter: function(v) { return v ? formatPrice(v) + " $/м²" : ""; } },
        { path: "detail.price_metrics.price_per_ha_uah", label: "Ціна за сотку (грн)", priority: 8, description: "Ціна за сотку у гривнях", formatter: function(v) { return v ? formatPrice(v / 100) + " ₴/с" : ""; } },
        { path: "detail.price_metrics.price_per_ha_usd", label: "Ціна за сотку (USD)", priority: 9, description: "Ціна за сотку у доларах США", formatter: function(v) { return v ? formatPrice(v / 100) + " $/с" : ""; } },
        { path: "url", label: "Посилання на оголошення", priority: 10, description: "Пряме посилання на OLX", isLink: true },
        { path: "detail.description", label: "Опис", priority: 11, description: "Детальний опис нерухомості" },
        { path: "detail.parameters", label: "Параметри", priority: 12, description: "Додаткові характеристики", isArray: true, arrayFormatter: function(item) {
          return formatParameterItem(item);
        }},
        { path: "detail.location", label: "Детальна локація", priority: 13, description: "Повна адреса", formatter: formatDetailLocation },
        { path: "detail.contact", label: "Контакти", priority: 14, description: "Інформація про продавця" },
        { path: "search_data.date_text", label: "Дата публікації", priority: 15, description: "Коли опубліковано оголошення" },
        { path: "search_data.currency", label: "Валюта", priority: 16, description: "Валюта ціни" },
      ]
    },
    prozorro: {
      hidden: ["_id", "created_at", "last_updated", "version_hash", "description_hash"],
      fields: [
        { path: "auction_data.title.uk_UA", label: "Назва", priority: 1, description: "Назва аукціону" },
        { path: "auction_data.title.en_US", label: "Назва (англ.)", priority: 2, description: "Назва англійською", fallback: true },
        { path: "auction_id", label: "ID аукціону", priority: 3, description: "Унікальний ідентифікатор" },
        { path: "auction_data.value.amount", label: "Стартова ціна (грн)", priority: 4, description: "Початкова ціна аукціону", formatter: function(v) { return formatPrice(v) + " ₴"; } },
        { path: "auction_data.price_metrics.total_price_usd", label: "Стартова ціна (USD)", priority: 5, description: "Початкова ціна в доларах США", formatter: function(v) { return v ? formatPrice(v) + " $" : ""; } },
        { path: "auction_data.price_metrics.price_per_m2_uah", label: "Ціна за м² (грн)", priority: 6, description: "Ціна за квадратний метр у гривнях", formatter: function(v) { return v ? formatPrice(v) + " ₴/м²" : ""; } },
        { path: "auction_data.price_metrics.price_per_m2_usd", label: "Ціна за м² (USD)", priority: 7, description: "Ціна за квадратний метр у доларах США", formatter: function(v) { return v ? formatPrice(v) + " $/м²" : ""; } },
        { path: "auction_data.price_metrics.price_per_ha_uah", label: "Ціна за сотку (грн)", priority: 8, description: "Ціна за сотку у гривнях", formatter: function(v) { return v ? formatPrice(v / 100) + " ₴/с" : ""; } },
        { path: "auction_data.price_metrics.price_per_ha_usd", label: "Ціна за сотку (USD)", priority: 9, description: "Ціна за сотку у доларах США", formatter: function(v) { return v ? formatPrice(v / 100) + " $/с" : ""; } },
        { path: "auction_data.value.currency", label: "Валюта", priority: 10, description: "Валюта ціни" },
        { path: "auction_data.value.valueAddedTaxIncluded", label: "ПДВ включено", priority: 6, description: "Чи включений податок", formatter: function(v) { return v ? "Так" : "Ні"; } },
        { path: "auction_data.status", label: "Статус", priority: 11, description: "Поточний статус аукціону" },
        { path: "auction_data.dateCreated", label: "Дата створення", priority: 12, description: "Коли створено аукціон", formatter: function(v) { return formatDate(v); } },
        { path: "auction_data.dateModified", label: "Дата оновлення", priority: 13, description: "Останнє оновлення", formatter: function(v) { return formatDate(v); } },
        { path: "auction_data.procedureType", label: "Тип процедури", priority: 14, description: "Тип аукціону" },
        { path: "auction_data.procuringEntity.name", label: "Організатор", priority: 15, description: "Хто проводить аукціон" },
        { path: "auction_data.description.uk_UA", label: "Опис", priority: 16, description: "Детальний опис", fallback: true },
        { path: "auction_data.description.en_US", label: "Опис (англ.)", priority: 17, description: "Опис англійською", fallback: true },
        { path: "auction_data.items", label: "Предмети аукціону", priority: 18, description: "Список предметів", isArray: true, arrayFormatter: formatProzorroItem },
        { path: "auction_data.bids", label: "Заявки", priority: 19, description: "Подані заявки", isArray: true, arrayFormatter: formatProzorroBid },
        { path: "auction_data.auctionPeriod.startDate", label: "Дата початку торгів", priority: 20, description: "Коли починаються торги", formatter: function(v) { return formatDate(v); }, fallback: true },
        { path: "auction_data.auctionPeriod.endDate", label: "Дата закінчення торгів", priority: 21, description: "Коли закінчуються торги", formatter: function(v) { return formatDate(v); }, fallback: true },
        { path: "auction_data.enquiryPeriod.endDate", label: "Термін подачі документів", priority: 22, description: "Дедлайн для документів", formatter: function(v) { return formatDate(v); }, fallback: true },
      ]
    }
  };

  function formatParameterItem(item) {
    if (item === null || item === undefined) return "";
    if (typeof item === "string" || typeof item === "number") return String(item);
    if (typeof item !== "object") return "";
    function toDisplayString(x) {
      if (x === null || x === undefined) return "";
      if (typeof x === "string" || typeof x === "number") return String(x);
      if (typeof x === "object" && x !== null) {
        var v = x.uk_UA || x.en_US || x.name || x.raw;
        if (v != null && typeof v === "string") return v;
      }
      return "";
    }
    var label = toDisplayString(item.label);
    var val = toDisplayString(item.value);
    if (!label && !val) return "";
    return label ? (label + ": " + (val || "—")) : val;
  }

  // Функція для отримання значення за шляхом (dot notation)
  function getNestedValue(obj, path) {
    var parts = path.split(".");
    var current = obj;
    for (var i = 0; i < parts.length; i++) {
      if (current === null || current === undefined) return null;
      current = current[parts[i]];
    }
    return current;
  }

  // Функція для рекурсивного відображення вкладених об'єктів у вигляді структурованого HTML
  function renderNestedObject(obj, container, level) {
    if (level === undefined) level = 0;
    if (level > 5) {
      // Захист від занадто глибокої вкладеності
      var pre = document.createElement("pre");
      pre.className = "detail-json-text";
      pre.textContent = JSON.stringify(obj, null, 2);
      container.appendChild(pre);
      return;
    }

    if (obj === null) {
      container.appendChild(document.createTextNode("null"));
      return;
    }

    if (Array.isArray(obj)) {
      if (obj.length === 0) {
        container.appendChild(document.createTextNode("[]"));
        return;
      }
      
      // Для великих масивів (більше 10 елементів) додаємо можливість згортання
      var isCollapsible = obj.length > 10 && level === 0;
      var list = document.createElement("ul");
      list.className = "detail-nested-list";
      
      if (isCollapsible) {
        var toggleBtn = document.createElement("button");
        toggleBtn.className = "detail-toggle-btn";
        toggleBtn.type = "button";
        toggleBtn.textContent = "▼ Розгорнути (" + obj.length + " елементів)";
        toggleBtn.onclick = function() {
          var isExpanded = list.style.display !== "none";
          list.style.display = isExpanded ? "none" : "block";
          toggleBtn.textContent = isExpanded ? "▼ Розгорнути (" + obj.length + " елементів)" : "▲ Згорнути";
        };
        container.appendChild(toggleBtn);
        list.style.display = "none";
      }
      
      obj.forEach(function(item, index) {
        var li = document.createElement("li");
        li.className = "detail-nested-item";
        if (typeof item === "object" && item !== null) {
          var itemLabel = document.createElement("span");
          itemLabel.className = "detail-nested-index";
          itemLabel.textContent = "[" + index + "]: ";
          li.appendChild(itemLabel);
          var itemContainer = document.createElement("div");
          itemContainer.className = "detail-nested-object";
          renderNestedObject(item, itemContainer, level + 1);
          li.appendChild(itemContainer);
        } else {
          li.textContent = String(item);
        }
        list.appendChild(li);
      });
      container.appendChild(list);
      return;
    }

    if (typeof obj === "object") {
      var keys = Object.keys(obj);
      if (keys.length === 0) {
        container.appendChild(document.createTextNode("{}"));
        return;
      }
      
      // Для великих об'єктів (більше 5 ключів) додаємо можливість згортання
      var isCollapsible = keys.length > 5 && level === 0;
      var objContainer = document.createElement("div");
      objContainer.className = "detail-nested-object";
      
      if (isCollapsible) {
        var toggleBtn = document.createElement("button");
        toggleBtn.className = "detail-toggle-btn";
        toggleBtn.type = "button";
        toggleBtn.textContent = "▼ Розгорнути (" + keys.length + " полів)";
        toggleBtn.onclick = function() {
          var isExpanded = contentDiv.style.display !== "none";
          contentDiv.style.display = isExpanded ? "none" : "block";
          toggleBtn.textContent = isExpanded ? "▼ Розгорнути (" + keys.length + " полів)" : "▲ Згорнути";
        };
        container.appendChild(toggleBtn);
        
        var contentDiv = document.createElement("div");
        contentDiv.className = "detail-nested-content";
        contentDiv.style.display = "none";
        
        keys.forEach(function(key) {
          var propDiv = document.createElement("div");
          propDiv.className = "detail-nested-property";
          var keySpan = document.createElement("span");
          keySpan.className = "detail-nested-key";
          keySpan.textContent = key + ": ";
          propDiv.appendChild(keySpan);
          var valueContainer = document.createElement("span");
          valueContainer.className = "detail-nested-value";
          var value = obj[key];
          if (typeof value === "object" && value !== null) {
            renderNestedObject(value, valueContainer, level + 1);
          } else if (value === null) {
            valueContainer.textContent = "null";
            valueContainer.className += " detail-null";
          } else if (typeof value === "string") {
            valueContainer.textContent = '"' + value + '"';
            valueContainer.className += " detail-string";
          } else if (typeof value === "number") {
            valueContainer.textContent = String(value);
            valueContainer.className += " detail-number";
          } else if (typeof value === "boolean") {
            valueContainer.textContent = value ? "true" : "false";
            valueContainer.className += " detail-boolean";
          } else {
            valueContainer.textContent = String(value);
          }
          propDiv.appendChild(valueContainer);
          contentDiv.appendChild(propDiv);
        });
        
        objContainer.appendChild(contentDiv);
        container.appendChild(objContainer);
      } else {
        keys.forEach(function(key) {
          var propDiv = document.createElement("div");
          propDiv.className = "detail-nested-property";
          var keySpan = document.createElement("span");
          keySpan.className = "detail-nested-key";
          keySpan.textContent = key + ": ";
          propDiv.appendChild(keySpan);
          var valueContainer = document.createElement("span");
          valueContainer.className = "detail-nested-value";
          var value = obj[key];
          if (typeof value === "object" && value !== null) {
            renderNestedObject(value, valueContainer, level + 1);
          } else if (value === null) {
            valueContainer.textContent = "null";
            valueContainer.className += " detail-null";
          } else if (typeof value === "string") {
            valueContainer.textContent = '"' + value + '"';
            valueContainer.className += " detail-string";
          } else if (typeof value === "number") {
            valueContainer.textContent = String(value);
            valueContainer.className += " detail-number";
          } else if (typeof value === "boolean") {
            valueContainer.textContent = value ? "true" : "false";
            valueContainer.className += " detail-boolean";
          } else {
            valueContainer.textContent = String(value);
          }
          propDiv.appendChild(valueContainer);
          objContainer.appendChild(propDiv);
        });
        container.appendChild(objContainer);
      }
      return;
    }

    // Прості типи
    container.appendChild(document.createTextNode(String(obj)));
  }

  // Функція для отримання головного фото з URL (для OLX)
  function getMainImageUrl(url, detailData) {
    if (!url || typeof url !== "string") return null;
    
    // Спочатку перевіряємо чи є фото в detail
    if (detailData && detailData.main_image && typeof detailData.main_image === "string") {
      return detailData.main_image;
    }
    if (detailData && detailData.images && Array.isArray(detailData.images) && detailData.images.length > 0) {
      return detailData.images[0];
    }
    
    // Можна додати ендпоінт для отримання фото через API
    // Зараз повертаємо null, якщо фото немає в даних
    return null;
  }

  // Функція для отримання фото з auction_data (для Prozorro)
  function getProzorroImageUrl(auctionData) {
    if (!auctionData || typeof auctionData !== "object") return null;
    // Шукаємо фото в items або інших полях
    var items = auctionData.items;
    if (Array.isArray(items) && items.length > 0) {
      for (var i = 0; i < items.length; i++) {
        var item = items[i];
        if (item.images && Array.isArray(item.images) && item.images.length > 0) {
          return item.images[0];
        }
        if (item.photo && typeof item.photo === "string") {
          return item.photo;
        }
      }
    }
    return null;
  }

  function renderDetailField(data, fieldConfig, container, type) {
    var path = fieldConfig.path;
    var value;
    if (path === "_detail_link" && fieldConfig.linkKey === "auction_id") {
      var aid = data.auction_id;
      value = aid ? "https://prozorro.sale/auction/" + aid : null;
    } else {
      value = getNestedValue(data, path);
    }
    if ((value === null || value === undefined || value === "") && !fieldConfig.fallback) return;
    if ((value === null || value === undefined || value === "") && fieldConfig.fallback) return;

    var field = document.createElement("div");
    field.className = "detail-field" + (fieldConfig.block ? " detail-field-block" : "");
    var labelEl = document.createElement("div");
    labelEl.className = "detail-field-label";
    labelEl.textContent = fieldConfig.label + ":";
    field.appendChild(labelEl);
    var valueEl = document.createElement("div");
    valueEl.className = "detail-field-value";

    if (fieldConfig.isLink && (typeof value === "string" || value)) {
      var link = document.createElement("a");
      link.href = typeof value === "string" ? value : ("https://prozorro.sale/auction/" + (value || ""));
      link.target = "_blank";
      link.textContent = "Відкрити";
      link.className = "detail-link";
      valueEl.appendChild(link);
    } else if (fieldConfig.isArray && Array.isArray(value)) {
      if (value.length === 0) return;
      var list = document.createElement("ul");
      list.className = "detail-array-list";
      value.forEach(function(item, index) {
        var text;
        if (fieldConfig.arrayFormatter) {
          text = fieldConfig.arrayFormatter(item, index);
        } else {
          text = typeof item === "object" && item !== null ? JSON.stringify(item) : String(item);
        }
        if (text === null || text === undefined || (typeof text === "string" && text.indexOf("[object Object]") !== -1)) return;
        if (text === "") return;
        var li = document.createElement("li");
        li.className = "detail-array-item";
        li.textContent = text;
        list.appendChild(li);
      });
      if (list.children.length === 0) return;
      valueEl.appendChild(list);
    } else {
      if (fieldConfig.formatter && typeof fieldConfig.formatter === "function") {
        valueEl.textContent = fieldConfig.formatter(value);
      } else if (typeof value === "object" && value !== null) {
        renderNestedObject(value, valueEl, 0);
      } else {
        valueEl.textContent = String(value);
      }
    }
    field.appendChild(valueEl);
    container.appendChild(field);
  }

  function renderAnalyticsTab(panel, data, type) {
    var source = type === "olx" ? "olx" : "prozorro";
    var sourceId = type === "olx" ? (data.url || data.source_id || "") : (data.auction_id || data.source_id || "");
    var city = "", region = "";
    if (type === "prozorro") {
      var refs = (data.auction_data && data.auction_data.address_refs) || [];
      if (refs.length && refs[0]) {
        city = (refs[0].city && refs[0].city.name) || "";
        region = (refs[0].region && refs[0].region.name) || "";
      }
      if (!city && data.auction_data && data.auction_data.items && data.auction_data.items[0]) {
        var addr = data.auction_data.items[0].address;
        if (addr && addr.locality) city = (addr.locality.uk_UA || addr.locality.en_US || "") || "";
        if (addr && addr.region) region = (addr.region.uk_UA || addr.region.en_US || "") || "";
      }
    } else if (type === "olx") {
      var loc = (data.search_data && data.search_data.location) || (data.detail && data.detail.location) || "";
      if (typeof loc === "string") {
        var parts = loc.split(",").map(function(s) { return s.trim(); });
        if (parts.length >= 1) city = parts[0];
        if (parts.length >= 2) region = parts[1];
      } else if (loc && loc.city) city = loc.city;
    }
    if (!city && !region) {
      panel.innerHTML = "<p class='detail-tab-empty'>Для порівняння цін потрібна інформація про місто або область. Можна сформувати детальний розбір через LLM.</p>";
      if (sourceId) {
        var genBtn = document.createElement("button");
        genBtn.className = "btn detail-analytics-generate-btn";
        genBtn.textContent = "Сформувати аналітику";
        genBtn.onclick = function() { _generateListingAnalytics(panel, data, type, source, sourceId); };
        panel.appendChild(genBtn);
      }
      return;
    }
    panel.innerHTML = "<div class='detail-analytics-loading'>Завантаження аналітики...</div>";
    var params = new URLSearchParams();
    params.append("period_type", "month");
    params.append("limit", "20");
    if (region) params.append("region", region);
    if (city) params.append("city", city);
    var aggregatesPromise = fetch("/api/analytics/aggregates?" + params.toString(), { headers: apiHeaders() })
      .then(function(r) { return r.ok ? r.json() : Promise.reject(new Error("Помилка")); });
    var listingAnalyticsPromise = sourceId
      ? fetch("/api/analytics/listing?source=" + encodeURIComponent(source) + "&source_id=" + encodeURIComponent(sourceId), { headers: apiHeaders() })
          .then(function(r) { return r.ok ? r.json() : { analysis_text: null }; })
          .catch(function() { return { analysis_text: null }; })
      : Promise.resolve({ analysis_text: null });
    Promise.all([aggregatesPromise, listingAnalyticsPromise])
      .then(function(results) {
        var res = results[0];
        var listingAnalytics = results[1];
        var items = (res.items || []).filter(function(x) {
          if (!x || !x.metrics) return false;
          var m = x.metrics;
          return (m.price_per_m2_uah && m.price_per_m2_uah.avg) || (m.price_per_ha_uah && m.price_per_ha_uah.avg) || (m.price_uah && m.price_uah.avg);
        });
        panel.innerHTML = "";
        var indicator = data.price_notes ? null : (data.price_indicator || null);
        var indicatorSource = data.price_indicator_source || null;
        var priceNotes = data.price_notes || null;
        var localityName = [city, region].filter(Boolean).join(", ") || region || city || "локації";
        var scopeText = indicatorSource === "city" ? "у місті" : indicatorSource === "region" ? "в області" : "у місцевості";

        function pickMetricKey(rows) {
          var m2 = 0, ha = 0, uah = 0;
          rows.forEach(function(row) {
            var m = row.metrics || {};
            if (m.price_per_m2_uah && m.price_per_m2_uah.avg) m2++;
            if (m.price_per_ha_uah && m.price_per_ha_uah.avg) ha++;
            if (m.price_uah && m.price_uah.avg) uah++;
          });
          if (m2 > 0) return "price_per_m2_uah";
          if (ha > 0) return "price_per_ha_uah";
          return "price_uah";
        }
        var metricKey = pickMetricKey(items);
        var unitMap = { price_per_m2_uah: "грн/м²", price_per_ha_uah: "грн/с", price_uah: "грн" };
        var unit = unitMap[metricKey] || "";
        function getMetric(row) {
          var m = (row.metrics || {})[metricKey];
          return m && m.avg ? { val: m.avg, count: m.count } : { val: 0, count: 0 };
        }
        var relevantItems = items.filter(function(row) { return getMetric(row).val > 0; });
        var totalCount = relevantItems.reduce(function(sum, row) { return sum + (getMetric(row).count || 0); }, 0);
        var avgVal = relevantItems.length ? relevantItems.reduce(function(s, r) { return s + getMetric(r).val; }, 0) / relevantItems.length : 0;

        var listingPrice = null;
        if (metricKey === "price_per_m2_uah") {
          listingPrice = type === "olx" ? getNestedValue(data, "detail.price_metrics.price_per_m2_uah") : getNestedValue(data, "auction_data.price_metrics.price_per_m2_uah");
        } else if (metricKey === "price_per_ha_uah") {
          listingPrice = type === "olx" ? getNestedValue(data, "detail.price_metrics.price_per_ha_uah") : getNestedValue(data, "auction_data.price_metrics.price_per_ha_uah");
        } else if (metricKey === "price_uah") {
          listingPrice = type === "olx" ? getNestedValue(data, "search_data.price_value") : getNestedValue(data, "auction_data.value.amount");
        }
        var cityContextNote = null;
        if (indicatorSource === "region" && listingPrice != null && typeof listingPrice === "number" && avgVal > 0 && city && totalCount > 0 && totalCount < 5) {
          var ratio = listingPrice / avgVal;
          if (ratio < 1 && (indicator === "аномально висока" || indicator === "дорога" || indicator === "вище середньої")) {
            cityContextNote = "Ми маємо небагато (" + totalCount + ") даних із міста, але якщо орієнтуватись по них — ціна по цій місцевості нижче середньої. Це може вказувати на те, що дана локація має переваги в межах області. З накопиченням масиву оголошень аналітика буде більш точна.";
          } else if (ratio > 1.3 && (indicator === "аномально низька" || indicator === "вигідна")) {
            cityContextNote = "Ми маємо небагато (" + totalCount + ") даних із міста, але якщо орієнтуватись по них — ціна по цій місцевості вища за середню. З накопиченням масиву оголошень аналітика буде більш точна.";
          }
        }

        var block = document.createElement("div");
        block.className = "detail-analytics-text";

        var descP = document.createElement("p");
        descP.className = "detail-analytics-desc";
        if (items.length === 0) {
          descP.textContent = "Немає достатніх даних для порівняння цін у цій місцевості. Порівнюємо за одиницю площі (грн/м² або грн/с). Для детального розбору ціни, місцезнаходження та оточення натисніть «Сформувати аналітику».";
        } else {
          var descParts = [];
          descParts.push("У " + localityName + " за останній місяць зареєстровано " + totalCount + " схожих оголошень.");
          if (avgVal > 0 && (metricKey === "price_per_m2_uah" || metricKey === "price_per_ha_uah")) {
            descParts.push("Середня ціна " + (metricKey === "price_per_m2_uah" ? "за м²" : "за га") + ": " + formatPrice(Math.round(avgVal)) + " " + unit + ".");
          } else if (avgVal > 0 && metricKey === "price_uah") {
            descParts.push("Середня абсолютна ціна: " + formatPrice(Math.round(avgVal)) + " грн (для об'єктів без площі).");
          }
          if (priceNotes) {
            descParts.push(priceNotes);
          } else if (indicator) {
            if (indicator === "вигідна") {
              descParts.push("Ціна цього об'єкта відносно низька серед аналогів — нижча за 25% оголошень " + scopeText + ".");
            } else if (indicator === "нижче середньої") {
              descParts.push("Ціна нижча за середню — у діапазоні 25–62% оголошень " + scopeText + ". Можливо, вигідний варіант.");
            } else if (indicator === "середня") {
              descParts.push("Ціна в середині діапазону — типове значення для подібних об'єктів " + scopeText + " (62–87% оголошень).");
            } else if (indicator === "вище середньої") {
              descParts.push("Ціна вища за середню — у верхній частині діапазону " + scopeText + " (87–100% оголошень).");
            } else if (indicator === "дорога") {
              descParts.push("Ціна вища за більшість аналогів — вище за 75% оголошень " + scopeText + ".");
            } else if (indicator === "аномально низька") {
              descParts.push("Ціна значно нижча за типовий діапазон " + scopeText + ". Можливі причини: потребує ремонту, обмеження у використанні, терміновість продажу або помилка в оголошенні. Рекомендуємо перевірити деталі.");
            } else if (indicator === "аномально висока") {
              descParts.push("Ціна значно вища за типовий діапазон " + scopeText + ". Можливі причини: преміум-локація, особливі характеристики, помилка в оголошенні. Рекомендуємо порівняти з іншими пропозиціями.");
            }
            if (cityContextNote) {
              descParts.push(cityContextNote);
            }
          } else {
            descParts.push("Для детального розбору ціни, місцезнаходження та оточення натисніть «Сформувати аналітику».");
          }
          descP.textContent = descParts.join(" ");
        }
        block.appendChild(descP);

        if (listingAnalytics && listingAnalytics.analysis_text) {
          var llmBlock = document.createElement("div");
          llmBlock.className = "detail-analytics-llm";
          var llmTitle = document.createElement("h4");
          llmTitle.className = "detail-analytics-llm-title";
          llmTitle.textContent = "Детальний розбір";
          llmBlock.appendChild(llmTitle);
          var llmP = document.createElement("p");
          llmP.className = "detail-analytics-llm-text";
          llmP.textContent = listingAnalytics.analysis_text;
          llmBlock.appendChild(llmP);
          block.appendChild(llmBlock);
        }

        if (sourceId) {
          var btnWrap = document.createElement("div");
          btnWrap.className = "detail-analytics-btn-wrap";
          var genBtn = document.createElement("button");
          genBtn.className = "btn detail-analytics-generate-btn";
          genBtn.textContent = listingAnalytics && listingAnalytics.analysis_text ? "Оновити аналітику" : "Сформувати аналітику";
          genBtn.onclick = function() { _generateListingAnalytics(panel, data, type, source, sourceId); };
          btnWrap.appendChild(genBtn);
          block.appendChild(btnWrap);
        }

        if (priceNotes || indicator) {
          var tagSection = document.createElement("div");
          tagSection.className = "detail-analytics-tag-section";
          var tagTitle = document.createElement("h4");
          tagTitle.className = "detail-analytics-tag-title";
          var tagLabel = indicator === "вигідна" ? "Вигідна ціна" : indicator === "нижче середньої" ? "Нижче середньої" : indicator === "середня" ? "Середня" : indicator === "вище середньої" ? "Вище середньої" : indicator === "дорога" ? "Дорога" : indicator === "аномально низька" ? "Аномально низька" : indicator === "аномально висока" ? "Аномально висока" : indicator;
          var tagScope = indicatorSource === "city" ? " (місто)" : indicatorSource === "region" ? " (область)" : "";
          tagTitle.textContent = priceNotes ? priceNotes : ("Що означає тег «" + tagLabel + tagScope + "»?");
          tagSection.appendChild(tagTitle);
          var tagP = document.createElement("p");
          tagP.className = "detail-analytics-tag-desc";
          var tagTexts = {
            "вигідна": "Ціна нижча за 25% схожих оголошень " + scopeText + ". Можливо, вигідний варіант.",
            "нижче середньої": "Ціна у діапазоні 25–62% (2–2,5 квадриль) — нижча за медіану, але не екстремально низька. Можливо, вигідний варіант.",
            "середня": "Ціна у середині діапазону (2,5–3,5 квадриль) — типове значення для подібних об'єктів " + scopeText + ".",
            "вище середньої": "Ціна у діапазоні 87–100% (3,5–4 квадриль) — вища за медіану, але в межах типових значень.",
            "дорога": "Ціна вища за 75% аналогів " + scopeText + ". Можливо, є особливості, що виправдовують вартість.",
            "аномально низька": "Ціна значно виходить за межі типових значень " + scopeText + " (нижча за статистичну «нижню межу»). Варто перевірити стан об'єкта, обмеження та умови угоди.",
            "аномально висока": "Ціна значно перевищує типовий діапазон " + scopeText + ". Можливо, об'єкт має унікальні переваги або є помилка в оголошенні."
          };
          tagP.textContent = priceNotes ? "Ціна позначена як така, що потребує перевірки. Можлива помилка парсингу або незвична умова угоди." : (tagTexts[indicator] || "Оцінка відносності ціни серед схожих об'єктів " + scopeText + ".");
          tagSection.appendChild(tagP);
          block.appendChild(tagSection);
        }

        panel.appendChild(block);
      })
      .catch(function() {
        panel.innerHTML = "<p class='detail-tab-empty'>Не вдалося завантажити аналітику.</p>";
      });
  }

  function _generateListingAnalytics(panel, data, type, source, sourceId) {
    panel.innerHTML = "<div class='detail-analytics-loading'>Формування аналітики...</div>";
    fetch("/api/analytics/listing/generate", {
      method: "POST",
      headers: Object.assign({ "Content-Type": "application/json" }, apiHeaders()),
      body: JSON.stringify({ source: source, source_id: sourceId, force: true }),
    })
      .then(function(r) {
        if (!r.ok) return r.json().then(function(j) { throw new Error(j.detail || "Помилка"); });
        return r.json();
      })
      .then(function() {
        renderAnalyticsTab(panel, data, type);
      })
      .catch(function(err) {
        panel.innerHTML = "<p class='detail-tab-empty'>Помилка: " + (err.message || "Не вдалося сформувати аналітику") + ".</p>";
      });
  }

  function renderRealEstateTab(panel, data, type) {
    var source = type === "olx" ? "olx" : "prozorro";
    var sourceId = type === "olx" ? (data.url || data.source_id || "") : (data.auction_id || data.source_id || "");
    if (!sourceId) {
      panel.innerHTML = "<p class='detail-tab-empty'>Немає даних для завантаження об'єктів.</p>";
      return;
    }
    panel.innerHTML = "<div class='detail-analytics-loading'>Завантаження об'єктів нерухомості...</div>";
    var params = new URLSearchParams();
    params.append("source", source);
    params.append("source_id", sourceId);
    fetch("/api/search/real-estate-objects?" + params.toString(), { headers: apiHeaders() })
      .then(function(r) {
        if (r.ok) return r.json();
        if (r.status === 404) return Promise.reject(new Error("NOT_FOUND"));
        return Promise.reject(new Error("Помилка"));
      })
      .then(function(res) {
        var items = res.items || [];
        panel.innerHTML = "";
        if (items.length === 0) {
          panel.innerHTML = "<p class='detail-tab-empty'>Немає пов'язаних об'єктів нерухомості.</p>";
          return;
        }
        var typeLabels = { land_plot: "Земельна ділянка", building: "Будівля", premises: "Приміщення" };
        items.forEach(function(obj) {
          var card = document.createElement("div");
          card.className = "detail-reo-card"; 
          var typeLabel = typeLabels[obj.type] || obj.type;
          var title = document.createElement("div");
          title.className = "detail-reo-card-title";
          title.textContent = typeLabel + (obj.description ? ": " + obj.description : "");
          card.appendChild(title);
          if (obj.area_sqm || obj.area_by_cadastre_sqm) {
            var area = document.createElement("div");
            area.className = "detail-reo-card-area";
            var areaTxt = "";
            if (obj.area_sqm) areaTxt = "Площа: " + obj.area_sqm + " м²";
            if (obj.area_by_cadastre_sqm) {
              if (areaTxt) areaTxt += " ";
              areaTxt += "(за кадастром: " + obj.area_by_cadastre_sqm + " м²)";
              area.classList.add("detail-reo-card-area-cadastre-diff");
            }
            area.textContent = areaTxt;
            card.appendChild(area);
          }
          if (obj.cadastral_info && obj.cadastral_info.cadastral_number) {
            var cad = document.createElement("div");
            cad.className = "detail-reo-card-cadastral";
            cad.textContent = "Кадастр: " + obj.cadastral_info.cadastral_number;
            card.appendChild(cad);
          }
          if (obj.address && obj.address.formatted_address) {
            var addr = document.createElement("div");
            addr.className = "detail-reo-card-address";
            addr.textContent = obj.address.formatted_address;
            card.appendChild(addr);
          }
          if (obj.floor != null) {
            var floor = document.createElement("div");
            floor.className = "detail-reo-card-floor";
            floor.textContent = "Поверх: " + obj.floor;
            card.appendChild(floor);
          }
          if (obj.premises_type) {
            var pt = document.createElement("div");
            pt.className = "detail-reo-card-premises-type";
            pt.textContent = "Тип: " + obj.premises_type;
            card.appendChild(pt);
          }
          if (obj.building_type) {
            var bt = document.createElement("div");
            bt.className = "detail-reo-card-building-type";
            bt.textContent = "Тип будівлі: " + obj.building_type;
            card.appendChild(bt);
          }
          if (obj.type === "building") {
            if (obj.land_plots && obj.land_plots.length > 0) {
              var lpBlock = document.createElement("div");
              lpBlock.className = "detail-reo-card-related";
              lpBlock.innerHTML = "<strong>Земельні ділянки:</strong>";
              var lpList = document.createElement("ul");
              lpList.className = "detail-reo-related-list";
              obj.land_plots.forEach(function(lp) {
                var li = document.createElement("li");
                var txt = lp.description || "Земельна ділянка";
                if (lp.cadastral_info && lp.cadastral_info.cadastral_number) txt += " (" + lp.cadastral_info.cadastral_number + ")";
                if (lp.area_sqm) txt += " — " + lp.area_sqm + " м²";
                if (lp.area_by_cadastre_sqm) txt += " (за кадастром: " + lp.area_by_cadastre_sqm + " м²)";
                li.textContent = txt;
                if (lp.area_by_cadastre_sqm) li.classList.add("detail-reo-cadastre-diff");
                lpList.appendChild(li);
              });
              lpBlock.appendChild(lpList);
              card.appendChild(lpBlock);
            }
            if (obj.premises && obj.premises.length > 0) {
              var premBlock = document.createElement("div");
              premBlock.className = "detail-reo-card-related";
              premBlock.innerHTML = "<strong>Приміщення:</strong>";
              var premList = document.createElement("ul");
              premList.className = "detail-reo-related-list";
              obj.premises.forEach(function(p) {
                var li = document.createElement("li");
                var txt = p.description || "Приміщення";
                if (p.floor != null) txt += ", поверх " + p.floor;
                if (p.premises_type) txt += ", " + p.premises_type;
                if (p.area_sqm) txt += " — " + p.area_sqm + " м²";
                li.textContent = txt;
                premList.appendChild(li);
              });
              premBlock.appendChild(premList);
              card.appendChild(premBlock);
            }
          }
          if (obj.type === "premises" && obj.building) {
            var bBlock = document.createElement("div");
            bBlock.className = "detail-reo-card-related";
            var bTxt = obj.building.description || "Будівля";
            if (obj.building.address && obj.building.address.formatted_address) bTxt += " — " + obj.building.address.formatted_address;
            bBlock.innerHTML = "<strong>Будівля:</strong> " + bTxt;
            card.appendChild(bBlock);
          }
          panel.appendChild(card);
        });
      })
      .catch(function(err) {
        var msg = err && err.message === "NOT_FOUND"
          ? "Оголошення не знайдено в зведеній таблиці. Запустіть оновлення даних для синхронізації."
          : "Не вдалося завантажити об'єкти нерухомості.";
        panel.innerHTML = "<p class='detail-tab-empty'>" + msg + "</p>";
      });
  }

  function renderDetail(data, type) {
    var detailContent = document.getElementById("detail-content");
    if (!detailContent) return;
    detailContent.innerHTML = "";

    var tabConfig = TAB_FIELD_CONFIG[type];
    if (!tabConfig) {
      detailContent.innerHTML = "<div class='error'>Невідомий тип: " + type + "</div>";
      return;
    }

    var wrap = document.createElement("div");
    wrap.className = "detail-page";

    // Головне фото
    var imageUrl = null;
    if (type === "olx") {
      imageUrl = getMainImageUrl(data.url, data.detail);
    } else if (type === "prozorro") {
      imageUrl = getProzorroImageUrl(data.auction_data);
    }
    if (imageUrl) {
      var imgWrap = document.createElement("div");
      imgWrap.className = "detail-hero-image";
      var img = document.createElement("img");
      img.src = imageUrl;
      img.alt = "";
      img.className = "detail-main-image";
      img.onerror = function() { imgWrap.remove(); };
      imgWrap.appendChild(img);
      wrap.appendChild(imgWrap);
    }

    // Hero-блок: назва, ціна, статус, локація
    var hero = document.createElement("div");
    hero.className = "detail-hero";
    var title = "";
    var priceText = "";
    var status = "";
    var location = "";
    var pageUrl = "";

    if (type === "prozorro") {
      title = getNestedValue(data, "auction_data.title.uk_UA") || getNestedValue(data, "auction_data.title.en_US") || "Аукціон";
      var amt = getNestedValue(data, "auction_data.value.amount");
      if (amt != null) {
        priceText = formatPrice(amt) + " ₴";
        var usd = getNestedValue(data, "auction_data.price_metrics.total_price_usd");
        if (usd) priceText += " (~" + formatPrice(usd) + " $)";
      }
      status = getNestedValue(data, "auction_data.status") || "";
      var refs = (data.auction_data && data.auction_data.address_refs) || [];
      if (refs.length && refs[0]) {
        var parts = [];
        if (refs[0].city && refs[0].city.name) parts.push(refs[0].city.name);
        if (refs[0].region && refs[0].region.name) parts.push(refs[0].region.name);
        location = parts.join(", ");
      }
      if (!location && data.auction_data && data.auction_data.items && data.auction_data.items[0]) {
        var addr = data.auction_data.items[0].address;
        if (addr) {
          var locParts = [];
          if (addr.locality && (addr.locality.uk_UA || addr.locality.en_US)) locParts.push(addr.locality.uk_UA || addr.locality.en_US || "");
          if (addr.region && (addr.region.uk_UA || addr.region.en_US)) locParts.push(addr.region.uk_UA || addr.region.en_US || "");
          location = locParts.filter(Boolean).join(", ");
        }
      }
      var aid = data.auction_id;
      pageUrl = aid ? "https://prozorro.sale/auction/" + aid : "";
    } else if (type === "olx") {
      var rawTitle = getNestedValue(data, "search_data.title") || "Оголошення";
      var olxUsd = getNestedValue(data, "detail.price_metrics.total_price_usd");
      title = fixTitlePriceDisplay(rawTitle, olxUsd) || rawTitle;
      var pv = getNestedValue(data, "search_data.price_value");
      if (pv != null) {
        priceText = formatPrice(pv) + " ₴";
        var usd = getNestedValue(data, "detail.price_metrics.total_price_usd");
        if (usd) priceText += " (~" + formatPrice(usd) + " $)";
      } else {
        var pt = getNestedValue(data, "search_data.price_text");
        if (pt) priceText = pt;
      }
      var loc = getNestedValue(data, "search_data.location");
      location = typeof loc === "string" ? loc : (loc && loc.city ? loc.city + (loc.region ? ", " + loc.region : "") : "");
      pageUrl = data.url || "";
    }

    var titleEl = document.createElement("h1");
    titleEl.className = "detail-hero-title";
    titleEl.textContent = title;
    hero.appendChild(titleEl);

    if (priceText) {
      var priceEl = document.createElement("div");
      priceEl.className = "detail-hero-price";
      priceEl.textContent = priceText;
      hero.appendChild(priceEl);
    }
    var badgeToShow = (SHOW_ANALYTICS_UI && data.price_indicator) ? data.price_indicator : (data.price_notes || null);
    var badgeSource = data.price_indicator_source || null;
    if (badgeToShow) {
      var indLabels = { "вигідна": "✓ Вигідна", "нижче середньої": "↓ Нижче середньої", "середня": "— Середня", "вище середньої": "↑ Вище середньої", "дорога": "↑ Дорога", "аномально низька": "⚠ Аномально низька", "аномально висока": "⚠ Аномально висока" };
      var isIndicator = indLabels.hasOwnProperty(badgeToShow);
      var heroBadge = document.createElement("span");
      heroBadge.className = "search-item-price-indicator search-item-price-" + (isIndicator ? badgeToShow.replace(/\s+/g, "-") : "anomalous");
      var base = indLabels[badgeToShow] || badgeToShow;
      heroBadge.textContent = badgeSource === "city" ? base + " (місто)" : badgeSource === "region" ? base + " (область)" : base;
      hero.appendChild(heroBadge);
    }

    if (status && type === "prozorro") {
      var statusEl = document.createElement("span");
      statusEl.className = "detail-hero-status detail-status-" + (status.toLowerCase().replace(/\s+/g, "-"));
      statusEl.textContent = status;
      hero.appendChild(statusEl);
    }

    if (location) {
      var locEl = document.createElement("div");
      locEl.className = "detail-hero-location";
      locEl.textContent = location;
      hero.appendChild(locEl);
    }

    wrap.appendChild(hero);

    // Швидкі факти (key info cards)
    var keyInfo = document.createElement("div");
    keyInfo.className = "detail-key-info";
    var keyItems = [];

    if (type === "prozorro") {
      var floorVal = getNestedValue(data, "auction_data.floor");
      if (floorVal) keyItems.push({ icon: "🏢", label: "Поверх", value: String(floorVal) });
      var ppm2 = getNestedValue(data, "auction_data.price_metrics.price_per_m2_uah");
      if (ppm2) keyItems.push({ icon: "㎡", label: "Ціна за м²", value: formatPrice(ppm2) + " ₴/м²" });
      var ppha = getNestedValue(data, "auction_data.price_metrics.price_per_ha_uah");
      if (ppha) keyItems.push({ icon: "⊡", label: "Ціна за сотку", value: formatPrice(ppha / 100) + " ₴/с" });
      var bids = getNestedValue(data, "auction_data.bids");
      if (Array.isArray(bids) && bids.length > 0) {
        keyItems.push({ icon: "📋", label: "Заявок", value: String(bids.length) });
      }
      var startDate = getNestedValue(data, "auction_data.auctionPeriod.startDate");
      if (startDate) keyItems.push({ icon: "📅", label: "Початок торгів", value: formatDate(startDate) });
      var endDate = getNestedValue(data, "auction_data.auctionPeriod.endDate");
      if (endDate) keyItems.push({ icon: "⏱", label: "Кінець торгів", value: formatDate(endDate) });
      var org = getNestedValue(data, "auction_data.procuringEntity.name");
      if (org) keyItems.push({ icon: "🏛", label: "Організатор", value: org });
    } else if (type === "olx") {
      var floorVal = getNestedValue(data, "detail.llm.floor");
      if (floorVal) keyItems.push({ icon: "🏢", label: "Поверх", value: String(floorVal) });
      var area = getNestedValue(data, "search_data.area_m2");
      if (area) keyItems.push({ icon: "㎡", label: "Площа", value: area + " м²" });
      var ppm2 = getNestedValue(data, "detail.price_metrics.price_per_m2_uah");
      if (ppm2) keyItems.push({ icon: "㎡", label: "Ціна за м²", value: formatPrice(ppm2) + " ₴/м²" });
      var ppha = getNestedValue(data, "detail.price_metrics.price_per_ha_uah");
      if (ppha) keyItems.push({ icon: "⊡", label: "Ціна за сотку", value: formatPrice(ppha / 100) + " ₴/с" });
      var dateText = getNestedValue(data, "search_data.date_text");
      if (dateText) keyItems.push({ icon: "📅", label: "Опубліковано", value: dateText });
    }

    keyItems.slice(0, 6).forEach(function(item) {
      var card = document.createElement("div");
      card.className = "detail-key-card";
      var iconSpan = document.createElement("span");
      iconSpan.className = "detail-key-icon";
      iconSpan.textContent = item.icon;
      var labelSpan = document.createElement("span");
      labelSpan.className = "detail-key-label";
      labelSpan.textContent = item.label;
      var valueSpan = document.createElement("span");
      valueSpan.className = "detail-key-value";
      valueSpan.textContent = item.value;
      card.appendChild(iconSpan);
      card.appendChild(labelSpan);
      card.appendChild(valueSpan);
      keyInfo.appendChild(card);
    });
    if (keyItems.length > 0) wrap.appendChild(keyInfo);

    // CTA-кнопки
    var ctaWrap = document.createElement("div");
    ctaWrap.className = "detail-cta-wrap";
    if (pageUrl) {
      var cta = document.createElement("a");
      cta.href = pageUrl;
      cta.target = "_blank";
      cta.rel = "noopener noreferrer";
      cta.className = "detail-cta-btn";
      cta.textContent = type === "prozorro" ? "Відкрити на ProZorro.Sale" : "Відкрити на OLX";
      ctaWrap.appendChild(cta);
    }
    var aiBtn = document.createElement("button");
    aiBtn.type = "button";
    aiBtn.className = "btn detail-ai-btn";
    aiBtn.textContent = "Спитати у AI-помічника";
    aiBtn.title = "Відкрити чат з AI для аналізу цього оголошення";
    currentDetailContext = buildDetailContext(data, type);
    aiBtn.addEventListener("click", function () {
      openChatWithListingContext(currentDetailContext);
    });
    ctaWrap.appendChild(aiBtn);
    if (currentUser && currentUser.is_admin) {
      var reformatBtn = document.createElement("button");
      reformatBtn.type = "button";
      reformatBtn.className = "btn detail-reformat-btn";
      reformatBtn.textContent = "Переформатувати дані";
      reformatBtn.title = "Повторна обробка через LLM та геосервіси (тільки для адмінів)";
      reformatBtn.addEventListener("click", function () {
        var src = type || "";
        var srcId = (type === "olx" ? (data.url || "") : (data.auction_id || ""));
        if (!src || !srcId) return;
        reformatBtn.disabled = true;
        reformatBtn.textContent = "Обробка...";
        fetch("/api/admin/reformat-listing?source=" + encodeURIComponent(src) + "&source_id=" + encodeURIComponent(srcId), {
          method: "POST",
          headers: apiHeaders()
        }).then(function (r) { return r.json(); }).then(function (res) {
          reformatBtn.disabled = false;
          reformatBtn.textContent = "Переформатувати дані";
          if (res.success) {
            openListingInApp(src, srcId);
          } else {
            alert(res.message || "Помилка переформатування");
          }
        }).catch(function (err) {
          reformatBtn.disabled = false;
          reformatBtn.textContent = "Переформатувати дані";
          alert(err.message || "Помилка запиту");
        });
      });
      ctaWrap.appendChild(reformatBtn);
    }
    wrap.appendChild(ctaWrap);

    // Вкладки
    var tabBar = document.createElement("div");
    tabBar.className = "detail-tabs";
    var panelsWrap = document.createElement("div");
    panelsWrap.className = "detail-tab-panels";

    var tabIds = Object.keys(tabConfig);
    tabIds.forEach(function(tabId, idx) {
      var tabCfg = tabConfig[tabId];
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "detail-tab-btn" + (idx === 0 ? " active" : "");
      btn.textContent = tabCfg.label;
      btn.dataset.tab = tabId;
      tabBar.appendChild(btn);

      var panel = document.createElement("div");
      panel.className = "detail-tab-panel" + (idx === 0 ? " active" : "");
      panel.dataset.tab = tabId;

      if (tabCfg.special && tabId === "analytics") {
        renderAnalyticsTab(panel, data, type);
      } else if (tabCfg.special && tabId === "real_estate") {
        renderRealEstateTab(panel, data, type);
      } else if (tabCfg.fields) {
        tabCfg.fields.forEach(function(fc) {
          renderDetailField(data, fc, panel, type);
        });
      }
      panelsWrap.appendChild(panel);
    });

    tabBar.addEventListener("click", function(e) {
      var btn = e.target;
      if (btn.tagName !== "BUTTON" || !btn.dataset.tab) return;
      tabBar.querySelectorAll(".detail-tab-btn").forEach(function(b) { b.classList.remove("active"); });
      panelsWrap.querySelectorAll(".detail-tab-panel").forEach(function(p) { p.classList.remove("active"); });
      btn.classList.add("active");
      var pnl = panelsWrap.querySelector("[data-tab='" + btn.dataset.tab + "']");
      if (pnl) pnl.classList.add("active");
    });

    wrap.appendChild(tabBar);
    wrap.appendChild(panelsWrap);
    detailContent.appendChild(wrap);
  }

  var filterBuilderConfig = null;
  var filterTreeRoot = null;

  function createFilterTreeRoot() {
    return { type: "group", groupType: "and", items: [] };
  }

  function createDefaultFilterTree() {
    return {
      type: "group",
      groupType: "and",
      items: [
        { type: "element", field: "status", operator: "eq", value: "активне" },
        { type: "element", field: "source", operator: "eq", value: "olx" }
      ]
    };
  }

  function renderFilterTree(container, root, fields) {
    if (!container || !root || !fields) return;
    container.innerHTML = "";
    var fieldKeys = Object.keys(fields);

    function opLabel(op) {
      var l = { eq: "=", ne: "≠", gte: "≥", lte: "≤", gt: ">", lt: "<", contains: "містить", not_contains: "не містить" };
      return l[op] || op;
    }

    function addToolbar(parentEl, groupNode) {
      var bar = document.createElement("div");
      bar.className = "filter-tree-toolbar";
      function add(type, label) {
        var btn = document.createElement("button");
        btn.type = "button";
        btn.className = "btn btn-ghost btn-small filter-tree-add";
        btn.textContent = label;
        btn.addEventListener("click", function () {
          if (type === "element") groupNode.items.push({ type: "element", field: fieldKeys[0] || "", operator: "eq", value: "" });
          else if (type === "group") groupNode.items.push({ type: "group", groupType: "and", items: [] });
          else if (type === "geo") groupNode.items.push({ type: "geo", geoType: "region", operator: "inside", value: "" });
          renderFilterTree(container, filterTreeRoot, fields);
        });
        bar.appendChild(btn);
      }
      add("element", "+ Умова");
      add("group", "+ Група І");
      var btnOr = document.createElement("button");
      btnOr.type = "button";
      btnOr.className = "btn btn-ghost btn-small filter-tree-add";
      btnOr.textContent = "+ Група АБО";
      btnOr.addEventListener("click", function () {
        groupNode.items.push({ type: "group", groupType: "or", items: [] });
        renderFilterTree(container, filterTreeRoot, fields);
      });
      bar.appendChild(btnOr);
      add("geo", "+ Гео");
      parentEl.appendChild(bar);
    }

    function renderGroup(parentEl, groupNode, depth) {
      var wrap = document.createElement("div");
      wrap.className = "filter-tree-group";
      wrap.style.setProperty("--depth", depth);

      var head = document.createElement("div");
      head.className = "filter-tree-group-head";
      var sel = document.createElement("select");
      sel.className = "filter-select filter-tree-group-type";
      sel.innerHTML = "<option value='and'>І (AND)</option><option value='or'>АБО (OR)</option>";
      sel.value = groupNode.groupType || "and";
      sel.addEventListener("change", function () { groupNode.groupType = sel.value; });
      head.appendChild(sel);
      if (depth > 0) {
        var del = document.createElement("button");
        del.type = "button";
        del.className = "btn btn-ghost btn-small filter-tree-del";
        del.textContent = "−";
        del.title = "Видалити групу";
        del.addEventListener("click", function () {
          var par = findParentGroup(filterTreeRoot, groupNode);
          if (par && par.items) {
            var i = par.items.indexOf(groupNode);
            if (i !== -1) { par.items.splice(i, 1); renderFilterTree(container, filterTreeRoot, fields); }
          }
        });
        head.appendChild(del);
      }
      wrap.appendChild(head);

      var children = document.createElement("div");
      children.className = "filter-tree-children";

      groupNode.items.forEach(function (item, idx) {
        if (item.type === "element") {
          var row = document.createElement("div");
          row.className = "filter-tree-row filter-tree-element";
          var fs = document.createElement("select");
          fs.className = "filter-select fb-field";
          fieldKeys.forEach(function (k) {
            var o = document.createElement("option");
            o.value = k;
            o.textContent = (fields[k] && fields[k].label_uk) || k;
            fs.appendChild(o);
          });
          fs.value = item.field || fieldKeys[0];
          fs.addEventListener("change", function () {
            item.field = fs.value;
            item.value = "";
            renderFilterTree(container, filterTreeRoot, fields);
          });
          var os = document.createElement("select");
          os.className = "filter-select fb-op";
          ["eq", "ne", "gte", "lte", "gt", "lt", "contains", "not_contains"].forEach(function (op) {
            var o = document.createElement("option");
            o.value = op;
            o.textContent = opLabel(op);
            os.appendChild(o);
          });
          os.value = item.operator || "eq";
          os.addEventListener("change", function () { item.operator = os.value; });
          var valueCell = document.createElement("div");
          valueCell.className = "filter-tree-value-cell";
          var fieldKey = item.field || fieldKeys[0];
          if (fieldKey === "status") {
            var sel = document.createElement("select");
            sel.className = "filter-select fb-value";
            sel.innerHTML = "<option value='активне'>Так</option><option value='неактивне'>Ні</option>";
            sel.value = item.value === "неактивне" ? "неактивне" : "активне";
            sel.addEventListener("change", function () { item.value = sel.value; });
            valueCell.appendChild(sel);
          } else if (fieldKey === "source") {
            var sel = document.createElement("select");
            sel.className = "filter-select fb-value";
            sel.innerHTML = "<option value='olx'>OLX</option><option value='prozorro'>ProZorro</option>";
            sel.value = (item.value === "prozorro" ? "prozorro" : "olx");
            sel.addEventListener("change", function () { item.value = sel.value; });
            valueCell.appendChild(sel);
          } else if (fieldKey === "property_type") {
            var propOptions = [
              "Комерційна нерухомість",
              "Земельна ділянка",
              "Земельна ділянка з нерухомістю",
              "Земля під будівництво",
              "Землі с/г призначення",
              "Нерухомість",
              "інше"
            ];
            var sel = document.createElement("select");
            sel.className = "filter-select fb-value";
            propOptions.forEach(function (opt) {
              var o = document.createElement("option");
              o.value = opt;
              o.textContent = opt;
              sel.appendChild(o);
            });
            if (item.value && propOptions.indexOf(item.value) !== -1) sel.value = item.value;
            else if (propOptions.length) { sel.value = propOptions[0]; item.value = propOptions[0]; }
            sel.addEventListener("change", function () { item.value = sel.value; });
            valueCell.appendChild(sel);
          } else {
            var vi = document.createElement("input");
            vi.className = "filter-input fb-value";
            vi.placeholder = "значення";
            var isNum = fields[fieldKey] && (fields[fieldKey].value_type === "number");
            if (isNum) vi.type = "number";
            vi.value = item.value != null ? String(item.value) : "";
            vi.addEventListener("input", function () {
              item.value = isNum && vi.value !== "" ? parseFloat(vi.value) : vi.value.trim() || "";
            });
            valueCell.appendChild(vi);
          }
          var del = document.createElement("button");
          del.type = "button";
          del.className = "btn btn-ghost btn-small filter-tree-del";
          del.textContent = "−";
          del.addEventListener("click", function () {
            groupNode.items.splice(idx, 1);
            renderFilterTree(container, filterTreeRoot, fields);
          });
          row.appendChild(fs);
          row.appendChild(os);
          row.appendChild(valueCell);
          row.appendChild(del);
          children.appendChild(row);
        } else if (item.type === "group") {
          var gWrap = document.createElement("div");
          gWrap.className = "filter-tree-group-wrap";
          renderGroup(gWrap, item, depth + 1);
          children.appendChild(gWrap);
        } else if (item.type === "geo") {
          var row = document.createElement("div");
          row.className = "filter-tree-row filter-tree-geo";
          var geoType = item.geoType || "region";
          var gs = document.createElement("select");
          gs.className = "filter-select";
          gs.innerHTML = "<option value='region'>Область</option><option value='settlement'>Населений пункт</option><option value='city_district'>Район міста</option>";
          gs.value = geoType;
          gs.addEventListener("change", function () {
            item.geoType = gs.value;
            item.geoRegion = "";
            item.geoCity = "";
            item.value = "";
            renderFilterTree(container, filterTreeRoot, fields);
          });
          var os = document.createElement("select");
          os.className = "filter-select";
          os.innerHTML = "<option value='inside'>в межах</option><option value='not_inside'>не в межах</option>";
          os.value = item.operator || "inside";
          os.addEventListener("change", function () { item.operator = os.value; });
          row.appendChild(gs);
          row.appendChild(os);
          function addGeoCombobox(label, getUrl, currentVal, onSelect) {
            var wrap = document.createElement("div");
            wrap.className = "filter-tree-geo-combobox";
            var input = document.createElement("input");
            input.className = "filter-input";
            input.placeholder = label;
            input.value = currentVal != null ? String(currentVal) : "";
            var drop = document.createElement("div");
            drop.className = "filter-tree-combobox-dropdown";
            drop.setAttribute("role", "listbox");
            wrap.appendChild(input);
            wrap.appendChild(drop);
            var allOptions = [];
            function loadOptions(cb) {
              var url = typeof getUrl === "function" ? getUrl() : getUrl;
              if (!url) { allOptions = []; if (cb) cb(); return; }
              fetch(url, { headers: apiHeaders() })
                .then(function (r) { return r.ok ? r.json() : Promise.reject(new Error("Network error")); })
                .then(function (data) {
                  allOptions = (data.regions || data.cities || data.districts || []);
                  if (cb) cb();
                })
                .catch(function () { allOptions = []; if (cb) cb(); });
            }
            function showDropdown() {
              var q = (input.value || "").toLowerCase().trim();
              var filtered = q ? allOptions.filter(function (o) { return String(o).toLowerCase().indexOf(q) !== -1; }) : allOptions;
              drop.innerHTML = "";
              filtered.slice(0, 150).forEach(function (opt) {
                var el = document.createElement("div");
                el.className = "filter-tree-combobox-option";
                el.setAttribute("role", "option");
                el.textContent = opt;
                el.addEventListener("mousedown", function (e) { e.preventDefault(); });
                el.addEventListener("click", function () {
                  onSelect(opt);
                  input.value = opt;
                  drop.style.display = "none";
                });
                drop.appendChild(el);
              });
              drop.style.display = filtered.length ? "block" : "none";
            }
            input.addEventListener("focus", function () {
              if (allOptions.length === 0) loadOptions(showDropdown);
              else showDropdown();
            });
            input.addEventListener("input", function () { onSelect(input.value.trim()); if (allOptions.length) showDropdown(); });
            input.addEventListener("keyup", function () { if (allOptions.length) showDropdown(); });
            input.addEventListener("blur", function () { setTimeout(function () { drop.style.display = "none"; }, 200); });
            if (input.value && allOptions.length === 0) loadOptions();
            return wrap;
          }
          function addRegionSelect(selectedRegion, onChange) {
            var sel = document.createElement("select");
            sel.className = "filter-select";
            sel.innerHTML = "<option value=''>— Область —</option>";
            sel.value = selectedRegion || "";
            fetch("/api/search/unified/filters/regions", { headers: apiHeaders() })
              .then(function (r) { return r.ok ? r.json() : Promise.reject(); })
              .then(function (data) {
                (data.regions || []).forEach(function (r) {
                  var o = document.createElement("option");
                  o.value = r;
                  o.textContent = r;
                  sel.appendChild(o);
                });
                sel.value = selectedRegion || "";
              })
              .catch(function () {});
            sel.addEventListener("change", function () { onChange(sel.value); renderFilterTree(container, filterTreeRoot, fields); });
            return sel;
          }
          function addCitySelect(regionVal, selectedCity, onChange) {
            var sel = document.createElement("select");
            sel.className = "filter-select";
            sel.innerHTML = "<option value=''>— Місто —</option>";
            sel.value = selectedCity || "";
            if (!regionVal) return sel;
            var url = "/api/search/unified/filters/cities?region=" + encodeURIComponent(regionVal);
            fetch(url, { headers: apiHeaders() })
              .then(function (r) { return r.ok ? r.json() : Promise.reject(); })
              .then(function (data) {
                sel.innerHTML = "<option value=''>— Місто —</option>";
                (data.cities || []).forEach(function (c) {
                  var o = document.createElement("option");
                  o.value = c;
                  o.textContent = c;
                  sel.appendChild(o);
                });
                sel.value = selectedCity || "";
              })
              .catch(function () {});
            sel.addEventListener("change", function () { onChange(sel.value); renderFilterTree(container, filterTreeRoot, fields); });
            return sel;
          }
          if (geoType === "region") {
            row.appendChild(addGeoCombobox("Область", "/api/search/unified/filters/regions", item.value, function (v) { item.value = v; }));
          } else if (geoType === "settlement") {
            row.appendChild(addRegionSelect(item.geoRegion, function (v) { item.geoRegion = v; item.value = ""; }));
            row.appendChild(addGeoCombobox("Населений пункт", function () {
              return "/api/search/unified/filters/cities" + (item.geoRegion ? "?region=" + encodeURIComponent(item.geoRegion) : "");
            }, item.value, function (v) { item.value = v; }));
          } else {
            row.appendChild(addRegionSelect(item.geoRegion, function (v) { item.geoRegion = v; item.geoCity = ""; item.value = ""; }));
            row.appendChild(addCitySelect(item.geoRegion, item.geoCity, function (v) { item.geoCity = v; item.value = ""; }));
            row.appendChild(addGeoCombobox("Район міста", function () {
              return (item.geoRegion && item.geoCity) ? "/api/search/unified/filters/districts?region=" + encodeURIComponent(item.geoRegion) + "&city=" + encodeURIComponent(item.geoCity) : null;
            }, item.value, function (v) { item.value = v; }));
          }
          var del = document.createElement("button");
          del.type = "button";
          del.className = "btn btn-ghost btn-small filter-tree-del";
          del.textContent = "−";
          del.addEventListener("click", function () {
            groupNode.items.splice(idx, 1);
            renderFilterTree(container, filterTreeRoot, fields);
          });
          row.appendChild(del);
          children.appendChild(row);
        }
      });

      wrap.appendChild(children);
      addToolbar(wrap, groupNode);
      parentEl.appendChild(wrap);
    }

    function findParentGroup(rootNode, target) {
      if (rootNode.type === "group" && rootNode.items) {
        if (rootNode.items.indexOf(target) !== -1) return rootNode;
        for (var i = 0; i < rootNode.items.length; i++) {
          if (rootNode.items[i].type === "group") {
            var found = findParentGroup(rootNode.items[i], target);
            if (found) return found;
          }
        }
      }
      return null;
    }

    renderGroup(container, root, 0);
  }

  function openFilterBuilderModal() {
    var modal = document.getElementById("filter-builder-modal");
    var treeEl = document.getElementById("filter-builder-tree");
    if (!modal || !treeEl) return;
    treeEl.innerHTML = "";
    if (!filterTreeRoot || !filterTreeRoot.items || filterTreeRoot.items.length === 0) {
      filterTreeRoot = createDefaultFilterTree();
    }
    fetch("/api/search/filter-fields", { headers: apiHeaders() })
      .then(function (r) {
        if (!r.ok) throw new Error("Не вдалося завантажити поля");
        return r.json();
      })
      .then(function (config) {
        filterBuilderConfig = config;
        var fields = config.fields || {};
        renderFilterTree(treeEl, filterTreeRoot, fields);
        modal.classList.remove("hidden");
      })
      .catch(function (err) { alert(err.message || "Помилка"); });
  }

  function updateSearchSummaries() {
    var summaryEl = document.getElementById("search-filter-summary-text");
    var sortSummaryEl = document.getElementById("search-sort-summary-text");
    var filterStringEl = document.getElementById("filter-string");
    var sortFieldSelect = document.getElementById("search-sort-field");
    var sortOrderSelect = document.getElementById("search-sort-order");
    if (summaryEl && filterStringEl) {
      var s = (filterStringEl.value || "").trim();
      summaryEl.textContent = s ? (s.length > 60 ? s.slice(0, 57) + "…" : s) : "Рядок відборів не задано";
    }
    if (sortSummaryEl && sortFieldSelect && sortOrderSelect) {
      var fieldLabels = { source_updated_at: "За датою", price: "За ціною", title: "За назвою" };
      var orderLabels = { desc: "новіші", asc: "старіші" };
      var f = fieldLabels[sortFieldSelect.value] || sortFieldSelect.value;
      var o = orderLabels[sortOrderSelect.value] || sortOrderSelect.value;
      sortSummaryEl.textContent = f + ", " + o;
    }
  }

  function switchSearchSubtab(tabName) {
    var resultsTab = document.getElementById("search-subtab-results");
    var settingsTab = document.getElementById("search-subtab-settings");
    var resultsPane = document.getElementById("search-tab-results");
    var settingsPane = document.getElementById("search-tab-settings");
    if (tabName === "settings") {
      if (resultsTab) { resultsTab.classList.remove("active"); resultsTab.setAttribute("aria-selected", "false"); }
      if (settingsTab) { settingsTab.classList.add("active"); settingsTab.setAttribute("aria-selected", "true"); }
      if (resultsPane) resultsPane.classList.remove("active");
      if (settingsPane) settingsPane.classList.add("active");
    } else {
      if (settingsTab) { settingsTab.classList.remove("active"); settingsTab.setAttribute("aria-selected", "false"); }
      if (resultsTab) { resultsTab.classList.add("active"); resultsTab.setAttribute("aria-selected", "true"); }
      if (settingsPane) settingsPane.classList.remove("active");
      if (resultsPane) resultsPane.classList.add("active");
      updateSearchSummaries();
    }
  }

  function bindSearchEvents() {
    // Підвкладки: Результати | Фільтри та сортування
    var subtabResults = document.getElementById("search-subtab-results");
    var subtabSettings = document.getElementById("search-subtab-settings");
    if (subtabResults) {
      subtabResults.addEventListener("click", function () { switchSearchSubtab("results"); });
    }
    if (subtabSettings) {
      subtabSettings.addEventListener("click", function () { switchSearchSubtab("settings"); });
    }
    var openSettingsBtn = document.getElementById("search-open-settings-tab");
    if (openSettingsBtn) {
      openSettingsBtn.addEventListener("click", function () { switchSearchSubtab("settings"); });
    }
    var editSortBtn = document.getElementById("search-edit-sort-tab");
    if (editSortBtn) {
      editSortBtn.addEventListener("click", function () { switchSearchSubtab("settings"); });
    }
    var runFromResultsBtn = document.getElementById("search-run-from-results");
    if (runFromResultsBtn) {
      runFromResultsBtn.addEventListener("click", function () {
        searchState.currentPage = 0;
        performSearch();
      });
    }

    // Сортування
    var sortFieldSelect = document.getElementById("search-sort-field");
    var sortOrderSelect = document.getElementById("search-sort-order");
    if (sortFieldSelect) {
      sortFieldSelect.value = searchState.sortField;
      sortFieldSelect.addEventListener("change", function () {
        searchState.sortField = sortFieldSelect.value;
        searchState.currentPage = 0;
        updateSearchSummaries();
        performSearch();
      });
    }
    if (sortOrderSelect) {
      sortOrderSelect.value = searchState.sortOrder;
      sortOrderSelect.addEventListener("change", function () {
        searchState.sortOrder = sortOrderSelect.value;
        searchState.currentPage = 0;
        updateSearchSummaries();
        performSearch();
      });
    }

    // Очищення фільтрів
    var clearFilterStringBtn = document.getElementById("search-clear-filter-string");
    if (clearFilterStringBtn) {
      clearFilterStringBtn.addEventListener("click", function () {
        var filterStringEl = document.getElementById("filter-string");
        if (filterStringEl) filterStringEl.value = "";
        var filterStringErr = document.getElementById("filter-string-error");
        if (filterStringErr) { filterStringErr.textContent = ""; filterStringErr.classList.add("hidden"); }
        searchState.currentPage = 0;
        performSearch();
      });
    }
    
    // Пошук за рядком фільтрів
    var searchByFilterStringBtn = document.getElementById("search-by-filter-string");
    var filterStringErrorEl = document.getElementById("filter-string-error");
    if (searchByFilterStringBtn) {
      searchByFilterStringBtn.addEventListener("click", function () {
        if (filterStringErrorEl) { filterStringErrorEl.textContent = ""; filterStringErrorEl.classList.add("hidden"); }
        searchState.currentPage = 0;
        switchSearchSubtab("results");
        performSearch();
      });
    }

    // Створити фільтри — відкрити модалку конструктора
    var searchBuildFiltersBtn = document.getElementById("search-build-filters");
    if (searchBuildFiltersBtn) {
      searchBuildFiltersBtn.addEventListener("click", function () { openFilterBuilderModal(); });
    }
    var filterBuilderModal = document.getElementById("filter-builder-modal");
    var filterBuilderInsert = document.getElementById("filter-builder-insert");
    var filterBuilderCancel = document.getElementById("filter-builder-cancel");
    if (filterBuilderInsert) {
      filterBuilderInsert.addEventListener("click", function () {
        function nodeToApi(node) {
          if (node.type === "group") {
            return {
              type: "group",
              group_type: node.groupType || "and",
              items: (node.items || []).map(nodeToApi)
            };
          }
          if (node.type === "geo") {
            return {
              type: "geo",
              geo_type: node.geoType || "region",
              operator: node.operator || "inside",
              value: node.value != null ? String(node.value).trim() : ""
            };
          }
          if (node.type === "element") {
            var v = node.value;
            if (v === "true" || v === true) v = true;
            else if (v === "false" || v === false) v = false;
            else if (typeof v === "string" && /^-?[\d.]+$/.test(v)) v = parseFloat(v);
            else if (v === undefined || v === null) v = "";
            var f = node.field;
            if (!f || typeof f !== "string") f = (node.value === "prozorro" || node.value === "olx") ? "source" : "status";
            return { type: "element", field: f, operator: node.operator || "eq", value: v };
          }
          return null;
        }
        var rootApi = filterTreeRoot ? {
          group_type: filterTreeRoot.groupType || "and",
          items: (filterTreeRoot.items || []).map(nodeToApi).filter(Boolean)
        } : { group_type: "and", items: [] };
        fetch("/api/search/filter-string-from-structure", {
          method: "POST",
          headers: Object.assign({ "Content-Type": "application/json" }, apiHeaders()),
          body: JSON.stringify({ root: rootApi })
        })
          .then(function (r) {
            if (!r.ok) return r.json().then(function (d) { throw new Error(d.detail || "Помилка"); });
            return r.json();
          })
          .then(function (data) {
            var ta = document.getElementById("filter-string");
            if (ta) ta.value = (data.filter_string != null ? data.filter_string : "");
            if (filterBuilderModal) filterBuilderModal.classList.add("hidden");
          })
          .catch(function (err) { alert(err.message || "Помилка формування рядка"); });
      });
    }
    if (filterBuilderCancel && filterBuilderModal) {
      filterBuilderCancel.addEventListener("click", function () { filterBuilderModal.classList.add("hidden"); });
    }
    
    // Назад з деталей
    var backBtn = document.getElementById("detail-back");
    if (backBtn) {
      backBtn.addEventListener("click", function () {
        showSearch();
      });
    }

    // Зберегти у файл (надсилається через бота — для мобільних)
    var saveToFileBtn = document.getElementById("search-save-to-file");
    if (saveToFileBtn) {
      saveToFileBtn.addEventListener("click", function () {
        var body = buildSearchExportBody();
        saveToFileBtn.disabled = true;
        var origText = saveToFileBtn.textContent;
        saveToFileBtn.textContent = "Надсилання...";
        fetch("/api/search/send-export-via-bot", {
          method: "POST",
          headers: apiHeaders(),
          body: JSON.stringify(body)
        })
          .then(function (r) {
            if (!r.ok) return r.json().then(function (d) { throw new Error(d.detail || "Помилка експорту"); });
            return r.json();
          })
          .then(function (data) {
            alert(data.message || "Файл надіслано в чат бота");
          })
          .catch(function (err) { alert(err.message || "Помилка"); })
          .finally(function () {
            saveToFileBtn.disabled = false;
            saveToFileBtn.textContent = origText;
          });
      });
    }

    // Зберегти як шаблон
    var saveAsTemplateBtn = document.getElementById("search-save-as-template");
    if (saveAsTemplateBtn) {
      saveAsTemplateBtn.addEventListener("click", function () {
        openReportConstructorFromSearch();
      });
    }
  }

  function buildSearchExportBody() {
    var filterStringEl = document.getElementById("filter-string");
    var sortFieldSelect = document.getElementById("search-sort-field");
    var sortOrderSelect = document.getElementById("search-sort-order");
    return {
      filter_string: filterStringEl ? filterStringEl.value.trim() || null : null,
      sort_field: sortFieldSelect ? sortFieldSelect.value : "source_updated_at",
      sort_order: sortOrderSelect ? sortOrderSelect.value : "desc"
    };
  }

  function openReportConstructorFromSearch() {
    var body = buildSearchExportBody();
    var prefill = {
      filter_string: body.filter_string,
      sort_field: body.sort_field,
      sort_order: body.sort_order
    };
    show("screen-files");
    var filesScreen = document.getElementById("screen-files");
    if (filesScreen) filesScreen.classList.remove("hidden");
    loadReportTemplates();
    setTimeout(function () { openReportConstructor(prefill); }, 100);
  }

  // Ініціалізація Telegram WebApp (викликається один раз)
  if (Tg) {
    if (Tg.ready) Tg.ready();
    if (Tg.expand) Tg.expand();
  }

  fetch("/api/me", { headers: apiHeaders() })
    .then(function (r) {
      if (!r.ok) {
        if (r.status === 403 && !initData) {
          throw new Error("Відкрийте застосунок з Telegram: кнопка меню бота або посилання t.me/YourBot?startapp");
        }
        throw new Error(r.status === 403 ? "Недійсні дані або не авторизовано" : "Помилка " + r.status);
      }
      return r.json();
    })
    .then(function (me) {
      currentUser = me;
      if (!me.authorized) {
        showError("Ваш користувач не авторизований. Зверніться до адміністратора.");
        // Все одно показуємо навігацію, щоб користувач міг бачити меню
        renderNav({ authorized: false });
        return;
      }
      renderNav(me);
      bindEvents(me);
      bindSearchEvents();
      initSearch();
      showSearch();
      renderChatMessages();
    })
    .catch(function (err) {
      console.error("Error loading profile:", err);
      currentUser = { authorized: false };
      // Показуємо навігацію навіть при помилці, щоб користувач міг бачити меню
      renderNav({ authorized: false });
      showError(err.message || "Не вдалося завантажити профіль.");
    });
})();
