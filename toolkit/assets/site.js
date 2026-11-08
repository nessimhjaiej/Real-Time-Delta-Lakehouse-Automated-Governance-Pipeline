// Site-wide behaviour: theme toggle, ?focus=<id> deep links, table filters.
(function () {
  "use strict";

  function storedTheme() {
    try { return localStorage.getItem("toolkit-theme"); } catch (e) { return null; }
  }
  function storeTheme(value) {
    try { localStorage.setItem("toolkit-theme", value); } catch (e) { /* ignore */ }
  }
  var saved = storedTheme();
  if (saved === "light" || saved === "dark") {
    document.documentElement.setAttribute("data-theme", saved);
  }

  document.addEventListener("DOMContentLoaded", function () {
    var toggle = document.querySelector('[data-role="theme"]');
    if (toggle) {
      toggle.addEventListener("click", function () {
        var current = document.documentElement.getAttribute("data-theme");
        if (!current) {
          current = window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
        }
        var next = current === "dark" ? "light" : "dark";
        document.documentElement.setAttribute("data-theme", next);
        storeTheme(next);
        document.dispatchEvent(new CustomEvent("toolkit:theme"));
      });
    }

    // Non-graph pages: scroll to and outline the element whose id is ?focus=.
    var focus = new URLSearchParams(window.location.search).get("focus");
    if (focus) {
      var target = document.getElementById(focus);
      if (target) {
        if (target.tagName === "DETAILS") { target.open = true; }
        var parentDetails = target.closest("details");
        if (parentDetails) { parentDetails.open = true; }
        target.classList.add("focused");
        target.scrollIntoView({ block: "start" });
      }
    }

    document.querySelectorAll("input[data-filter-target]").forEach(function (input) {
      var table = document.getElementById(input.getAttribute("data-filter-target"));
      if (!table) { return; }
      input.addEventListener("input", function () {
        var q = input.value.trim().toLowerCase();
        table.querySelectorAll("tbody tr").forEach(function (row) {
          row.hidden = q !== "" && row.textContent.toLowerCase().indexOf(q) === -1;
        });
      });
    });
  });
})();
