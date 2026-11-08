// Interactive graph widget used by every graph page (see graph_block in common.py).
// Click a node: highlight its upstream (orange) and downstream (green) chain,
// list both in the side panel. ?focus=<node id> selects a node on load.
(function () {
  "use strict";

  var PALETTE = ["--c1", "--c2", "--c3", "--c4", "--c5", "--c6", "--c7", "--c8", "--c9"];

  function cssVar(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }

  function el(tag, attrs, text) {
    var node = document.createElement(tag);
    if (attrs) {
      Object.keys(attrs).forEach(function (k) { node.setAttribute(k, attrs[k]); });
    }
    if (text !== undefined && text !== null) { node.textContent = String(text); }
    return node;
  }

  function labelSize(label) {
    var lines = String(label || "").split("\n");
    var longest = lines.reduce(function (m, l) { return Math.max(m, l.length); }, 0);
    return { w: Math.max(48, longest * 7 + 22), h: 16 * lines.length + 14 };
  }

  function groupColors(spec) {
    var keys = Object.keys(spec.groups || {});
    spec.nodes.forEach(function (n) {
      if (n.group && keys.indexOf(n.group) === -1) { keys.push(n.group); }
    });
    var colors = {};
    keys.forEach(function (k, i) { colors[k] = cssVar(PALETTE[i % PALETTE.length]); });
    return { keys: keys, colors: colors };
  }

  function buildStyle(colors) {
    var text = cssVar("--text");
    var surface = cssVar("--surface");
    var muted = cssVar("--muted");
    var edge = cssVar("--edge");
    var up = cssVar("--hl-up");
    var down = cssVar("--hl-down");
    var accent = cssVar("--accent");
    return [
      {
        selector: "node",
        style: {
          shape: "round-rectangle",
          label: "data(label)",
          "text-wrap": "wrap",
          "text-valign": "center",
          "text-halign": "center",
          "font-size": 12,
          "font-family": "system-ui, -apple-system, Segoe UI, sans-serif",
          color: text,
          width: "data(w)",
          height: "data(h)",
          "background-color": surface,
          "border-width": 2,
          "border-color": function (n) { return colors[n.data("group")] || muted; },
        },
      },
      {
        selector: ":parent",
        style: {
          "text-valign": "top",
          "text-halign": "center",
          "font-weight": 600,
          color: muted,
          "background-opacity": 0.04,
          "background-color": muted,
          "border-style": "dashed",
          "border-width": 1,
          "border-color": muted,
          padding: 14,
        },
      },
      {
        selector: "edge",
        style: {
          width: 1.4,
          "curve-style": "bezier",
          "line-color": edge,
          "target-arrow-color": edge,
          "target-arrow-shape": "triangle",
          "arrow-scale": 0.9,
          label: "data(label)",
          "font-size": 10,
          color: muted,
          "text-rotation": "autorotate",
          "text-background-color": surface,
          "text-background-opacity": 0.85,
          "text-background-padding": 2,
        },
      },
      { selector: ".faded", style: { opacity: parseFloat(cssVar("--fade")) || 0.14 } },
      { selector: "node.up", style: { "border-color": up, "border-width": 3 } },
      { selector: "node.down", style: { "border-color": down, "border-width": 3 } },
      { selector: "edge.up", style: { "line-color": up, "target-arrow-color": up, width: 2.4 } },
      { selector: "edge.down", style: { "line-color": down, "target-arrow-color": down, width: 2.4 } },
      {
        selector: "node.sel",
        style: { "border-color": accent, "border-width": 4, "font-weight": 700 },
      },
      { selector: "node.match", style: { "border-color": accent, "border-style": "double", "border-width": 5 } },
    ];
  }

  function mount(root) {
    var id = root.getAttribute("data-graph");
    var dataEl = document.getElementById("graph-" + id);
    if (!dataEl) { return; }
    var spec = JSON.parse(dataEl.textContent);
    var canvas = root.querySelector('[data-role="canvas"]');
    var panel = root.querySelector('[data-role="panel"]');
    var legend = root.querySelector('[data-role="legend"]');
    var search = root.querySelector('[data-role="search"]');
    var transitive = root.querySelector('[data-role="transitive"]');

    if (typeof window.cytoscape !== "function") {
      canvas.textContent = "The graph library could not load (it is fetched from cdn.jsdelivr.net). " +
        "The node and edge lists below the graph still show everything.";
      return;
    }
    if (window.cytoscapeDagre && !window.__toolkitDagre) {
      window.cytoscape.use(window.cytoscapeDagre);
      window.__toolkitDagre = true;
    }

    var byId = {};
    spec.nodes.forEach(function (n) { byId[n.id] = n; });
    var parents = {};
    spec.nodes.forEach(function (n) { if (n.parent) { parents[n.parent] = true; } });

    var elements = [];
    spec.nodes.forEach(function (n) {
      var size = labelSize(n.label || n.id);
      var data = { id: n.id, label: n.label || n.id, group: n.group || "", w: size.w, h: size.h };
      if (n.parent && byId[n.parent]) { data.parent = n.parent; }
      elements.push({ group: "nodes", data: data });
    });
    spec.edges.forEach(function (e, i) {
      if (!byId[e.source] || !byId[e.target]) { return; }
      elements.push({
        group: "edges",
        data: { id: "e" + i, source: e.source, target: e.target, label: e.label || "", kind: e.kind || "" },
      });
    });

    var palette = groupColors(spec);
    var cy = window.cytoscape({
      container: canvas,
      elements: elements,
      style: buildStyle(palette.colors),
      wheelSensitivity: 0.25,
      minZoom: 0.08,
      maxZoom: 3,
      layout: window.__toolkitDagre
        ? { name: "dagre", rankDir: spec.rankDir || "LR", nodeSep: 18, rankSep: 70, edgeSep: 8, animate: false }
        : { name: "breadthfirst", directed: true, spacingFactor: 1.1 },
    });

    // Legend with group toggles.
    palette.keys.forEach(function (key) {
      var label = el("label");
      var box = el("input", { type: "checkbox" });
      box.checked = true;
      var swatch = el("span", { class: "swatch" });
      swatch.style.background = palette.colors[key];
      label.appendChild(box);
      label.appendChild(swatch);
      label.appendChild(document.createTextNode((spec.groups && spec.groups[key]) || key));
      box.addEventListener("change", function () {
        cy.nodes().filter(function (n) { return n.data("group") === key; })
          .style("display", box.checked ? "element" : "none");
      });
      legend.appendChild(label);
    });

    function chain(node, dir) {
      if (transitive.checked) {
        return dir === "up" ? node.predecessors() : node.successors();
      }
      return dir === "up" ? node.incomers() : node.outgoers();
    }

    function clear() {
      cy.elements().removeClass("faded up down sel match");
      panel.innerHTML = "";
      panel.appendChild(el("p", { class: "muted" }, "Click a node to see what feeds it and what it feeds."));
    }

    function nodeList(title, cls, nodes) {
      var frag = document.createDocumentFragment();
      frag.appendChild(el("h4", { class: cls }, title + " (" + nodes.length + ")"));
      var ul = el("ul");
      nodes.sort(function (a, b) { return a.data("label").localeCompare(b.data("label")); })
        .forEach(function (n) {
          var li = el("li");
          var btn = el("button", { type: "button", title: n.id() }, n.data("label").replace(/\n/g, " "));
          btn.addEventListener("click", function () { select(n.id(), true); });
          li.appendChild(btn);
          ul.appendChild(li);
        });
      if (!nodes.length) { ul.appendChild(el("li", { class: "muted" }, "none")); }
      frag.appendChild(ul);
      return frag;
    }

    function select(nodeId, center) {
      var node = cy.getElementById(nodeId);
      if (!node || node.empty()) { return; }
      var up = chain(node, "up");
      var down = chain(node, "down");
      cy.elements().removeClass("faded up down sel match").addClass("faded");
      node.removeClass("faded").addClass("sel");
      node.ancestors().removeClass("faded");
      up.removeClass("faded").addClass("up");
      down.removeClass("faded").addClass("down");
      up.nodes().ancestors().removeClass("faded");
      down.nodes().ancestors().removeClass("faded");

      var info = byId[nodeId] || {};
      panel.innerHTML = "";
      panel.appendChild(el("h3", null, (info.label || nodeId).replace(/\n/g, " ")));
      panel.appendChild(el("p", { class: "muted small" }, nodeId));
      var detail = info.detail || {};
      var keys = Object.keys(detail);
      if (info.group) { keys.unshift("__group"); }
      if (keys.length) {
        var dl = el("dl");
        keys.forEach(function (k) {
          dl.appendChild(el("dt", null, k === "__group" ? "type" : k));
          dl.appendChild(el("dd", null, k === "__group"
            ? ((spec.groups && spec.groups[info.group]) || info.group) : detail[k]));
        });
        panel.appendChild(dl);
      }
      (info.links || []).forEach(function (lnk) {
        var p = el("p");
        var a = el("a", { href: lnk.href }, lnk.label);
        p.appendChild(a);
        panel.appendChild(p);
      });
      panel.appendChild(nodeList(spec.upLabel || "Upstream", "up-title", up.nodes().toArray()));
      panel.appendChild(nodeList(spec.downLabel || "Downstream", "down-title", down.nodes().toArray()));

      if (center) {
        cy.animate({ center: { eles: node }, zoom: Math.max(cy.zoom(), 0.8) }, { duration: 250 });
      }
      try {
        var url = new URL(window.location.href);
        url.searchParams.set("focus", nodeId);
        window.history.replaceState(null, "", url.toString());
      } catch (e) { /* file:// in some browsers */ }
    }

    var selected = null;
    cy.on("tap", "node", function (evt) {
      if (evt.target.isParent()) { return; }
      selected = evt.target.id();
      select(selected, false);
    });
    cy.on("tap", function (evt) { if (evt.target === cy) { selected = null; clear(); } });
    transitive.addEventListener("change", function () { if (selected) { select(selected, false); } });
    root.querySelector('[data-role="fit"]').addEventListener("click", function () { cy.fit(undefined, 30); });
    root.querySelector('[data-role="clear"]').addEventListener("click", function () { selected = null; clear(); cy.fit(undefined, 30); });

    search.addEventListener("input", function () {
      var q = search.value.trim().toLowerCase();
      cy.nodes().removeClass("match");
      if (!q) { return; }
      var hits = cy.nodes().filter(function (n) {
        return n.id().toLowerCase().indexOf(q) !== -1 || n.data("label").toLowerCase().indexOf(q) !== -1;
      });
      hits.addClass("match");
    });
    search.addEventListener("keydown", function (evt) {
      if (evt.key !== "Enter") { return; }
      var hit = cy.nodes(".match").filter(function (n) { return !n.isParent(); })[0];
      if (hit) { selected = hit.id(); select(selected, true); }
    });

    document.addEventListener("toolkit:theme", function () {
      palette = groupColors(spec);
      cy.style(buildStyle(palette.colors));
    });
    window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", function () {
      palette = groupColors(spec);
      cy.style(buildStyle(palette.colors));
    });

    cy.fit(undefined, 30);
    var focus = new URLSearchParams(window.location.search).get("focus");
    if (focus && byId[focus]) {
      selected = focus;
      select(focus, true);
      root.scrollIntoView({ block: "start" });
    }
    root.__cy = cy;
  }

  document.addEventListener("DOMContentLoaded", function () {
    document.querySelectorAll(".graph[data-graph]").forEach(mount);
  });
})();
