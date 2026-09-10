/* @ds-bundle: {"format":4,"namespace":"DeephavenDesignSystem_e929bc","components":[{"name":"Button","sourcePath":"components/controls/Button.jsx"},{"name":"IconButton","sourcePath":"components/controls/IconButton.jsx"},{"name":"Badge","sourcePath":"components/display/Badge.jsx"},{"name":"Card","sourcePath":"components/display/Card.jsx"},{"name":"Icon","sourcePath":"components/display/Icon.jsx"},{"name":"Tag","sourcePath":"components/display/Tag.jsx"},{"name":"Checkbox","sourcePath":"components/forms/Checkbox.jsx"},{"name":"Input","sourcePath":"components/forms/Input.jsx"},{"name":"Radio","sourcePath":"components/forms/Radio.jsx"},{"name":"Select","sourcePath":"components/forms/Select.jsx"},{"name":"Switch","sourcePath":"components/forms/Switch.jsx"},{"name":"Tabs","sourcePath":"components/navigation/Tabs.jsx"},{"name":"Dialog","sourcePath":"components/overlay/Dialog.jsx"},{"name":"Tooltip","sourcePath":"components/overlay/Tooltip.jsx"}],"sourceHashes":{"components/controls/Button.jsx":"ca19d5493cfd","components/controls/IconButton.jsx":"6e8e3635a6a6","components/display/Badge.jsx":"70d9e1102653","components/display/Card.jsx":"e01e2ce2aaaf","components/display/Icon.jsx":"08da3b922490","components/display/Tag.jsx":"214fdc3f66f0","components/forms/Checkbox.jsx":"7c4af346a075","components/forms/Input.jsx":"a99bed516dbe","components/forms/Radio.jsx":"95fa9e982594","components/forms/Select.jsx":"0f028788a779","components/forms/Switch.jsx":"10240a00ecfd","components/navigation/Tabs.jsx":"c19695ef6d34","components/overlay/Dialog.jsx":"b1fd1a903d2f","components/overlay/Tooltip.jsx":"ce67983b2206","ui_kits/console/AppShell.jsx":"8c52331da57d","ui_kits/console/Console.jsx":"538d5cf5b617","ui_kits/console/DataGrid.jsx":"1f7fca4aa6f4"},"inlinedExternals":[],"unexposedExports":[]} */

(() => {

const __ds_ns = (window.DeephavenDesignSystem_e929bc = window.DeephavenDesignSystem_e929bc || {});

const __ds_scope = {};

(__ds_ns.__errors = __ds_ns.__errors || []);

// components/controls/Button.jsx
try { (() => {
function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
/**
 * Deephaven Button — primary action control.
 * Variants: primary (brand blue), secondary (outline), ghost, danger.
 * Sizes: sm | md | lg. Theme-aware via CSS custom properties.
 */
function Button({
  children,
  variant = "primary",
  size = "md",
  disabled = false,
  fullWidth = false,
  iconLeft = null,
  iconRight = null,
  type = "button",
  onClick,
  style = {},
  ...rest
}) {
  const heights = {
    sm: "var(--dh-control-height-sm)",
    md: "var(--dh-control-height-md)",
    lg: "var(--dh-control-height-lg)"
  };
  const padding = {
    sm: "0 var(--dh-space-4)",
    md: "0 var(--dh-space-5)",
    lg: "0 var(--dh-space-6)"
  };
  const fontSize = {
    sm: "var(--dh-text-sm)",
    md: "var(--dh-text-base)",
    lg: "var(--dh-text-md)"
  };
  const base = {
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    gap: "var(--dh-space-3)",
    height: heights[size],
    padding: padding[size],
    width: fullWidth ? "100%" : "auto",
    fontFamily: "var(--dh-font-sans)",
    fontSize: fontSize[size],
    fontWeight: "var(--dh-weight-semibold)",
    lineHeight: 1,
    borderRadius: "var(--dh-radius-md)",
    border: "1px solid transparent",
    cursor: disabled ? "not-allowed" : "pointer",
    opacity: disabled ? 0.45 : 1,
    whiteSpace: "nowrap",
    transition: "background var(--dh-duration-fast) var(--dh-ease), border-color var(--dh-duration-fast) var(--dh-ease), color var(--dh-duration-fast) var(--dh-ease)",
    userSelect: "none"
  };
  const variants = {
    primary: {
      background: "var(--dh-accent)",
      color: "var(--dh-accent-contrast)"
    },
    secondary: {
      background: "transparent",
      color: "var(--dh-text-primary)",
      borderColor: "var(--dh-border-strong)"
    },
    ghost: {
      background: "transparent",
      color: "var(--dh-text-primary)"
    },
    danger: {
      background: "var(--dh-status-negative)",
      color: "var(--dh-white)"
    }
  };
  const hoverBg = {
    primary: "var(--dh-accent-hover)",
    secondary: "var(--dh-surface-hover)",
    ghost: "var(--dh-surface-hover)",
    danger: "var(--dh-negative-500)"
  };
  const activeBg = {
    primary: "var(--dh-accent-active)",
    secondary: "var(--dh-surface-hover)",
    ghost: "var(--dh-surface-hover)",
    danger: "var(--dh-negative-300)"
  };
  const [hover, setHover] = React.useState(false);
  const [active, setActive] = React.useState(false);
  const dyn = {};
  if (!disabled && hover) dyn.background = hoverBg[variant];
  if (!disabled && active) dyn.background = activeBg[variant];
  if (!disabled && hover && variant === "secondary") dyn.borderColor = "var(--dh-border-focus)";
  return /*#__PURE__*/React.createElement("button", _extends({
    type: type,
    disabled: disabled,
    onClick: onClick,
    onMouseEnter: () => setHover(true),
    onMouseLeave: () => {
      setHover(false);
      setActive(false);
    },
    onMouseDown: () => setActive(true),
    onMouseUp: () => setActive(false),
    style: {
      ...base,
      ...variants[variant],
      ...dyn,
      ...style
    }
  }, rest), iconLeft, children, iconRight);
}
Object.assign(__ds_scope, { Button });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/controls/Button.jsx", error: String((e && e.message) || e) }); }

// components/display/Badge.jsx
try { (() => {
function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
/**
 * Deephaven Badge — status/count label. Tones map to semantic colors.
 */
function Badge({
  children,
  tone = "neutral",
  variant = "soft",
  style = {},
  ...rest
}) {
  const tones = {
    neutral: {
      solid: ["var(--dh-fg-500)", "var(--dh-white)"],
      soft: ["var(--dh-bg-800)", "var(--dh-fg-300)"]
    },
    accent: {
      solid: ["var(--dh-accent)", "var(--dh-accent-contrast)"],
      soft: ["var(--dh-primary-1100)", "var(--dh-primary-400)"]
    },
    positive: {
      solid: ["var(--dh-status-positive)", "var(--dh-white)"],
      soft: ["color-mix(in srgb, var(--dh-status-positive) 18%, transparent)", "var(--dh-positive-200)"]
    },
    negative: {
      solid: ["var(--dh-status-negative)", "var(--dh-white)"],
      soft: ["color-mix(in srgb, var(--dh-status-negative) 15%, transparent)", "var(--dh-negative-300)"]
    },
    warn: {
      solid: ["var(--dh-warn-300)", "var(--dh-fg-100)"],
      soft: ["color-mix(in srgb, var(--dh-warn-500) 22%, transparent)", "var(--dh-warn-100)"]
    },
    info: {
      solid: ["var(--dh-status-info)", "var(--dh-white)"],
      soft: ["color-mix(in srgb, var(--dh-info-500) 15%, transparent)", "var(--dh-info-200)"]
    }
  };
  const [bg, fg] = tones[tone][variant];
  return /*#__PURE__*/React.createElement("span", _extends({
    style: {
      display: "inline-flex",
      alignItems: "center",
      gap: "var(--dh-space-2)",
      height: 20,
      padding: "0 var(--dh-space-3)",
      background: bg,
      color: fg,
      fontFamily: "var(--dh-font-sans)",
      fontSize: "var(--dh-text-xs)",
      fontWeight: "var(--dh-weight-semibold)",
      letterSpacing: "var(--dh-tracking-wide)",
      textTransform: "uppercase",
      borderRadius: "var(--dh-radius-sm)",
      whiteSpace: "nowrap",
      ...style
    }
  }, rest), children);
}
Object.assign(__ds_scope, { Badge });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/display/Badge.jsx", error: String((e && e.message) || e) }); }

// components/display/Card.jsx
try { (() => {
function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
/**
 * Deephaven Card — surface container for grouped content.
 */
function Card({
  children,
  title,
  actions,
  padding = "md",
  elevated = false,
  style = {},
  ...rest
}) {
  const pad = {
    none: 0,
    sm: "var(--dh-space-4)",
    md: "var(--dh-space-6)",
    lg: "var(--dh-space-7)"
  }[padding];
  return /*#__PURE__*/React.createElement("div", _extends({
    style: {
      background: "var(--dh-surface-card)",
      border: "1px solid var(--dh-border)",
      borderRadius: "var(--dh-radius-lg)",
      boxShadow: elevated ? "var(--dh-shadow-md)" : "none",
      color: "var(--dh-text-primary)",
      fontFamily: "var(--dh-font-sans)",
      overflow: "hidden",
      ...style
    }
  }, rest), (title || actions) && /*#__PURE__*/React.createElement("div", {
    style: {
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      gap: "var(--dh-space-4)",
      padding: "var(--dh-space-4) var(--dh-space-6)",
      borderBottom: "1px solid var(--dh-border)"
    }
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      fontSize: "var(--dh-text-md)",
      fontWeight: "var(--dh-weight-semibold)"
    }
  }, title), actions && /*#__PURE__*/React.createElement("div", {
    style: {
      display: "flex",
      gap: "var(--dh-space-2)"
    }
  }, actions)), /*#__PURE__*/React.createElement("div", {
    style: {
      padding: pad
    }
  }, children));
}
Object.assign(__ds_scope, { Card });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/display/Card.jsx", error: String((e && e.message) || e) }); }

// components/display/Icon.jsx
try { (() => {
function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
/**
 * Deephaven Icon — thin wrapper over Lucide (loaded from CDN).
 * Deephaven's product UI uses a clean line-icon set; Lucide matches the
 * ~1.75px stroke, rounded-cap style closely. Load once in the host page:
 *   <script src="https://unpkg.com/lucide@latest/dist/umd/lucide.min.js"></script>
 * then call lucide.createIcons() after mount (Icon does this automatically).
 */
function Icon({
  name,
  size = 16,
  strokeWidth = 2,
  color = "currentColor",
  style = {},
  ...rest
}) {
  const ref = React.useRef(null);
  React.useEffect(() => {
    if (window.lucide && ref.current) {
      // Replace the <i data-lucide> placeholder with an inline SVG.
      window.lucide.createIcons({
        nameAttr: "data-lucide",
        icons: window.lucide.icons
      });
    }
  }, [name, size, strokeWidth]);
  return /*#__PURE__*/React.createElement("i", _extends({
    ref: ref,
    "data-lucide": name,
    style: {
      display: "inline-flex",
      width: size,
      height: size,
      color,
      strokeWidth,
      flex: "0 0 auto",
      verticalAlign: "middle",
      ...style
    }
  }, rest));
}
Object.assign(__ds_scope, { Icon });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/display/Icon.jsx", error: String((e && e.message) || e) }); }

// components/controls/IconButton.jsx
try { (() => {
function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
/**
 * Deephaven IconButton — square, icon-only control for toolbars & compact UI.
 */
function IconButton({
  name,
  label,
  size = "md",
  variant = "ghost",
  disabled = false,
  active = false,
  onClick,
  style = {},
  ...rest
}) {
  const dim = {
    sm: 24,
    md: 32,
    lg: 40
  }[size];
  const glyph = {
    sm: 14,
    md: 16,
    lg: 18
  }[size];
  const [hover, setHover] = React.useState(false);
  const bg = active ? "var(--dh-surface-hover)" : hover && !disabled ? "var(--dh-surface-hover)" : "transparent";
  const color = active ? "var(--dh-accent)" : "var(--dh-text-secondary)";
  return /*#__PURE__*/React.createElement("button", _extends({
    type: "button",
    "aria-label": label,
    title: label,
    disabled: disabled,
    onClick: onClick,
    onMouseEnter: () => setHover(true),
    onMouseLeave: () => setHover(false),
    style: {
      display: "inline-flex",
      alignItems: "center",
      justifyContent: "center",
      width: dim,
      height: dim,
      padding: 0,
      border: variant === "outline" ? "1px solid var(--dh-border-strong)" : "1px solid transparent",
      borderRadius: "var(--dh-radius-md)",
      background: bg,
      color: hover && !disabled && !active ? "var(--dh-text-primary)" : color,
      cursor: disabled ? "not-allowed" : "pointer",
      opacity: disabled ? 0.4 : 1,
      transition: "background var(--dh-duration-fast) var(--dh-ease), color var(--dh-duration-fast) var(--dh-ease)",
      ...style
    }
  }, rest), /*#__PURE__*/React.createElement(__ds_scope.Icon, {
    name: name,
    size: glyph
  }));
}
Object.assign(__ds_scope, { IconButton });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/controls/IconButton.jsx", error: String((e && e.message) || e) }); }

// components/display/Tag.jsx
try { (() => {
function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
/**
 * Deephaven Tag — removable label / filter chip.
 */
function Tag({
  children,
  onRemove,
  mono = false,
  style = {},
  ...rest
}) {
  const [hover, setHover] = React.useState(false);
  return /*#__PURE__*/React.createElement("span", _extends({
    style: {
      display: "inline-flex",
      alignItems: "center",
      gap: "var(--dh-space-2)",
      height: 24,
      padding: onRemove ? "0 var(--dh-space-2) 0 var(--dh-space-3)" : "0 var(--dh-space-3)",
      background: "var(--dh-surface-hover)",
      color: "var(--dh-text-primary)",
      border: "1px solid var(--dh-border)",
      borderRadius: "var(--dh-radius-md)",
      fontFamily: mono ? "var(--dh-font-mono)" : "var(--dh-font-sans)",
      fontSize: "var(--dh-text-sm)",
      whiteSpace: "nowrap",
      ...style
    }
  }, rest), children, onRemove && /*#__PURE__*/React.createElement("button", {
    type: "button",
    onClick: onRemove,
    onMouseEnter: () => setHover(true),
    onMouseLeave: () => setHover(false),
    "aria-label": "Remove",
    style: {
      display: "inline-flex",
      alignItems: "center",
      justifyContent: "center",
      width: 16,
      height: 16,
      padding: 0,
      border: "none",
      borderRadius: "var(--dh-radius-sm)",
      background: hover ? "var(--dh-border-strong)" : "transparent",
      color: "var(--dh-text-secondary)",
      cursor: "pointer"
    }
  }, /*#__PURE__*/React.createElement(__ds_scope.Icon, {
    name: "x",
    size: 12,
    strokeWidth: 2.5
  })));
}
Object.assign(__ds_scope, { Tag });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/display/Tag.jsx", error: String((e && e.message) || e) }); }

// components/forms/Checkbox.jsx
try { (() => {
function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
/**
 * Deephaven Checkbox — supports checked, unchecked, indeterminate.
 */
function Checkbox({
  checked = false,
  indeterminate = false,
  onChange,
  label,
  disabled = false,
  style = {},
  ...rest
}) {
  const on = checked || indeterminate;
  const [hover, setHover] = React.useState(false);
  return /*#__PURE__*/React.createElement("label", {
    onMouseEnter: () => setHover(true),
    onMouseLeave: () => setHover(false),
    style: {
      display: "inline-flex",
      alignItems: "center",
      gap: "var(--dh-space-3)",
      cursor: disabled ? "not-allowed" : "pointer",
      opacity: disabled ? 0.5 : 1,
      fontFamily: "var(--dh-font-sans)",
      fontSize: "var(--dh-text-base)",
      color: "var(--dh-text-primary)",
      userSelect: "none",
      ...style
    }
  }, /*#__PURE__*/React.createElement("input", _extends({
    type: "checkbox",
    checked: checked,
    disabled: disabled,
    onChange: onChange,
    style: {
      position: "absolute",
      opacity: 0,
      width: 0,
      height: 0
    }
  }, rest)), /*#__PURE__*/React.createElement("span", {
    style: {
      display: "inline-flex",
      alignItems: "center",
      justifyContent: "center",
      width: 16,
      height: 16,
      flex: "0 0 auto",
      borderRadius: "var(--dh-radius-sm)",
      border: `1px solid ${on ? "var(--dh-accent)" : hover ? "var(--dh-border-focus)" : "var(--dh-border-strong)"}`,
      background: on ? "var(--dh-accent)" : "var(--dh-surface-card)",
      color: "var(--dh-accent-contrast)",
      transition: "background var(--dh-duration-fast) var(--dh-ease), border-color var(--dh-duration-fast) var(--dh-ease)"
    }
  }, indeterminate ? /*#__PURE__*/React.createElement(__ds_scope.Icon, {
    name: "minus",
    size: 12,
    strokeWidth: 3
  }) : checked ? /*#__PURE__*/React.createElement(__ds_scope.Icon, {
    name: "check",
    size: 12,
    strokeWidth: 3
  }) : null), label && /*#__PURE__*/React.createElement("span", null, label));
}
Object.assign(__ds_scope, { Checkbox });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/forms/Checkbox.jsx", error: String((e && e.message) || e) }); }

// components/forms/Input.jsx
try { (() => {
function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
/**
 * Deephaven Input — single-line text field.
 */
function Input({
  value,
  defaultValue,
  onChange,
  placeholder = "",
  type = "text",
  size = "md",
  disabled = false,
  invalid = false,
  mono = false,
  iconLeft = null,
  fullWidth = false,
  style = {},
  ...rest
}) {
  const [focus, setFocus] = React.useState(false);
  const height = {
    sm: "var(--dh-control-height-sm)",
    md: "var(--dh-control-height-md)",
    lg: "var(--dh-control-height-lg)"
  }[size];
  const fs = {
    sm: "var(--dh-text-sm)",
    md: "var(--dh-text-base)",
    lg: "var(--dh-text-md)"
  }[size];
  const borderColor = invalid ? "var(--dh-status-negative)" : focus ? "var(--dh-border-focus)" : "var(--dh-border-strong)";
  return /*#__PURE__*/React.createElement("div", {
    style: {
      display: "inline-flex",
      alignItems: "center",
      gap: "var(--dh-space-3)",
      height,
      width: fullWidth ? "100%" : "auto",
      padding: "0 var(--dh-space-4)",
      background: disabled ? "var(--dh-surface-sunken)" : "var(--dh-surface-card)",
      border: `1px solid ${borderColor}`,
      borderRadius: "var(--dh-radius-md)",
      boxShadow: focus && !invalid ? "0 0 0 2px color-mix(in srgb, var(--dh-border-focus) 30%, transparent)" : "none",
      transition: "border-color var(--dh-duration-fast) var(--dh-ease), box-shadow var(--dh-duration-fast) var(--dh-ease)",
      opacity: disabled ? 0.6 : 1,
      ...style
    }
  }, iconLeft && /*#__PURE__*/React.createElement("span", {
    style: {
      color: "var(--dh-text-muted)",
      display: "inline-flex"
    }
  }, iconLeft), /*#__PURE__*/React.createElement("input", _extends({
    value: value,
    defaultValue: defaultValue,
    onChange: onChange,
    placeholder: placeholder,
    type: type,
    disabled: disabled,
    onFocus: () => setFocus(true),
    onBlur: () => setFocus(false),
    style: {
      flex: 1,
      minWidth: 0,
      border: "none",
      outline: "none",
      background: "transparent",
      fontFamily: mono ? "var(--dh-font-mono)" : "var(--dh-font-sans)",
      fontSize: fs,
      color: "var(--dh-text-primary)"
    }
  }, rest)));
}
Object.assign(__ds_scope, { Input });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/forms/Input.jsx", error: String((e && e.message) || e) }); }

// components/forms/Radio.jsx
try { (() => {
function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
/**
 * Deephaven Radio — single option within a group.
 */
function Radio({
  checked = false,
  onChange,
  label,
  name,
  value,
  disabled = false,
  style = {},
  ...rest
}) {
  const [hover, setHover] = React.useState(false);
  return /*#__PURE__*/React.createElement("label", {
    onMouseEnter: () => setHover(true),
    onMouseLeave: () => setHover(false),
    style: {
      display: "inline-flex",
      alignItems: "center",
      gap: "var(--dh-space-3)",
      cursor: disabled ? "not-allowed" : "pointer",
      opacity: disabled ? 0.5 : 1,
      fontFamily: "var(--dh-font-sans)",
      fontSize: "var(--dh-text-base)",
      color: "var(--dh-text-primary)",
      userSelect: "none",
      ...style
    }
  }, /*#__PURE__*/React.createElement("input", _extends({
    type: "radio",
    name: name,
    value: value,
    checked: checked,
    disabled: disabled,
    onChange: onChange,
    style: {
      position: "absolute",
      opacity: 0,
      width: 0,
      height: 0
    }
  }, rest)), /*#__PURE__*/React.createElement("span", {
    style: {
      display: "inline-flex",
      alignItems: "center",
      justifyContent: "center",
      width: 16,
      height: 16,
      flex: "0 0 auto",
      borderRadius: "var(--dh-radius-pill)",
      border: `1px solid ${checked ? "var(--dh-accent)" : hover ? "var(--dh-border-focus)" : "var(--dh-border-strong)"}`,
      background: "var(--dh-surface-card)",
      transition: "border-color var(--dh-duration-fast) var(--dh-ease)"
    }
  }, checked && /*#__PURE__*/React.createElement("span", {
    style: {
      width: 8,
      height: 8,
      borderRadius: "var(--dh-radius-pill)",
      background: "var(--dh-accent)"
    }
  })), label && /*#__PURE__*/React.createElement("span", null, label));
}
Object.assign(__ds_scope, { Radio });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/forms/Radio.jsx", error: String((e && e.message) || e) }); }

// components/forms/Select.jsx
try { (() => {
function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
/**
 * Deephaven Select — native-backed dropdown styled to match the system.
 */
function Select({
  value,
  defaultValue,
  onChange,
  options = [],
  size = "md",
  disabled = false,
  fullWidth = false,
  style = {},
  ...rest
}) {
  const [focus, setFocus] = React.useState(false);
  const height = {
    sm: "var(--dh-control-height-sm)",
    md: "var(--dh-control-height-md)",
    lg: "var(--dh-control-height-lg)"
  }[size];
  const fs = {
    sm: "var(--dh-text-sm)",
    md: "var(--dh-text-base)",
    lg: "var(--dh-text-md)"
  }[size];
  return /*#__PURE__*/React.createElement("div", {
    style: {
      position: "relative",
      display: "inline-flex",
      alignItems: "center",
      height,
      width: fullWidth ? "100%" : "auto",
      opacity: disabled ? 0.6 : 1,
      ...style
    }
  }, /*#__PURE__*/React.createElement("select", _extends({
    value: value,
    defaultValue: defaultValue,
    onChange: onChange,
    disabled: disabled,
    onFocus: () => setFocus(true),
    onBlur: () => setFocus(false),
    style: {
      appearance: "none",
      WebkitAppearance: "none",
      width: "100%",
      height: "100%",
      padding: "0 var(--dh-space-7) 0 var(--dh-space-4)",
      fontFamily: "var(--dh-font-sans)",
      fontSize: fs,
      color: "var(--dh-text-primary)",
      background: disabled ? "var(--dh-surface-sunken)" : "var(--dh-surface-card)",
      border: `1px solid ${focus ? "var(--dh-border-focus)" : "var(--dh-border-strong)"}`,
      borderRadius: "var(--dh-radius-md)",
      outline: "none",
      cursor: disabled ? "not-allowed" : "pointer",
      boxShadow: focus ? "0 0 0 2px color-mix(in srgb, var(--dh-border-focus) 30%, transparent)" : "none",
      transition: "border-color var(--dh-duration-fast) var(--dh-ease)"
    }
  }, rest), options.map(o => {
    const opt = typeof o === "string" ? {
      value: o,
      label: o
    } : o;
    return /*#__PURE__*/React.createElement("option", {
      key: opt.value,
      value: opt.value
    }, opt.label);
  })), /*#__PURE__*/React.createElement("span", {
    style: {
      position: "absolute",
      right: "var(--dh-space-3)",
      pointerEvents: "none",
      color: "var(--dh-text-muted)",
      display: "inline-flex"
    }
  }, /*#__PURE__*/React.createElement(__ds_scope.Icon, {
    name: "chevron-down",
    size: 14
  })));
}
Object.assign(__ds_scope, { Select });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/forms/Select.jsx", error: String((e && e.message) || e) }); }

// components/forms/Switch.jsx
try { (() => {
function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
/**
 * Deephaven Switch — on/off toggle for settings.
 */
function Switch({
  checked = false,
  onChange,
  label,
  disabled = false,
  size = "md",
  style = {},
  ...rest
}) {
  const w = size === "sm" ? 28 : 36;
  const h = size === "sm" ? 16 : 20;
  const knob = h - 4;
  return /*#__PURE__*/React.createElement("label", {
    style: {
      display: "inline-flex",
      alignItems: "center",
      gap: "var(--dh-space-3)",
      cursor: disabled ? "not-allowed" : "pointer",
      opacity: disabled ? 0.5 : 1,
      fontFamily: "var(--dh-font-sans)",
      fontSize: "var(--dh-text-base)",
      color: "var(--dh-text-primary)",
      userSelect: "none",
      ...style
    }
  }, /*#__PURE__*/React.createElement("input", _extends({
    type: "checkbox",
    checked: checked,
    disabled: disabled,
    onChange: onChange,
    style: {
      position: "absolute",
      opacity: 0,
      width: 0,
      height: 0
    }
  }, rest)), /*#__PURE__*/React.createElement("span", {
    style: {
      position: "relative",
      width: w,
      height: h,
      flex: "0 0 auto",
      borderRadius: "var(--dh-radius-pill)",
      background: checked ? "var(--dh-accent)" : "var(--dh-border-strong)",
      transition: "background var(--dh-duration-base) var(--dh-ease)"
    }
  }, /*#__PURE__*/React.createElement("span", {
    style: {
      position: "absolute",
      top: 2,
      left: checked ? w - knob - 2 : 2,
      width: knob,
      height: knob,
      borderRadius: "var(--dh-radius-pill)",
      background: "var(--dh-white)",
      boxShadow: "var(--dh-shadow-sm)",
      transition: "left var(--dh-duration-base) var(--dh-ease)"
    }
  })), label && /*#__PURE__*/React.createElement("span", null, label));
}
Object.assign(__ds_scope, { Switch });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/forms/Switch.jsx", error: String((e && e.message) || e) }); }

// components/navigation/Tabs.jsx
try { (() => {
function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
/**
 * Deephaven Tabs — horizontal tab strip (underline style).
 */
function Tabs({
  tabs = [],
  value,
  onChange,
  style = {},
  ...rest
}) {
  return /*#__PURE__*/React.createElement("div", _extends({
    role: "tablist",
    style: {
      display: "flex",
      gap: "var(--dh-space-2)",
      borderBottom: "1px solid var(--dh-border)",
      fontFamily: "var(--dh-font-sans)",
      ...style
    }
  }, rest), tabs.map(t => {
    const tab = typeof t === "string" ? {
      value: t,
      label: t
    } : t;
    const selected = tab.value === value;
    return /*#__PURE__*/React.createElement(TabButton, {
      key: tab.value,
      tab: tab,
      selected: selected,
      onChange: onChange
    });
  }));
}
function TabButton({
  tab,
  selected,
  onChange
}) {
  const [hover, setHover] = React.useState(false);
  return /*#__PURE__*/React.createElement("button", {
    type: "button",
    role: "tab",
    "aria-selected": selected,
    onClick: () => onChange && onChange(tab.value),
    onMouseEnter: () => setHover(true),
    onMouseLeave: () => setHover(false),
    style: {
      position: "relative",
      display: "inline-flex",
      alignItems: "center",
      gap: "var(--dh-space-2)",
      height: 36,
      padding: "0 var(--dh-space-4)",
      border: "none",
      background: "transparent",
      color: selected ? "var(--dh-text-primary)" : hover ? "var(--dh-text-primary)" : "var(--dh-text-secondary)",
      fontFamily: "var(--dh-font-sans)",
      fontSize: "var(--dh-text-base)",
      fontWeight: selected ? "var(--dh-weight-semibold)" : "var(--dh-weight-medium)",
      cursor: "pointer",
      boxShadow: selected ? "inset 0 -2px 0 var(--dh-accent)" : "none",
      transition: "color var(--dh-duration-fast) var(--dh-ease)"
    }
  }, tab.count != null && /*#__PURE__*/React.createElement("span", {
    style: {
      fontSize: "var(--dh-text-xs)",
      fontFamily: "var(--dh-font-mono)",
      color: "var(--dh-text-muted)"
    }
  }, tab.count), tab.label);
}
Object.assign(__ds_scope, { Tabs });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/navigation/Tabs.jsx", error: String((e && e.message) || e) }); }

// components/overlay/Dialog.jsx
try { (() => {
function _extends() { return _extends = Object.assign ? Object.assign.bind() : function (n) { for (var e = 1; e < arguments.length; e++) { var t = arguments[e]; for (var r in t) ({}).hasOwnProperty.call(t, r) && (n[r] = t[r]); } return n; }, _extends.apply(null, arguments); }
/**
 * Deephaven Dialog — modal overlay for focused tasks & confirmations.
 * Renders inline (no portal) so it works in card previews; in an app,
 * mount at the root and toggle `open`.
 */
function Dialog({
  open = true,
  onClose,
  title,
  children,
  footer,
  width = 480,
  style = {},
  ...rest
}) {
  if (!open) return null;
  return /*#__PURE__*/React.createElement("div", {
    style: {
      position: "absolute",
      inset: 0,
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      background: "rgba(0, 9, 48, 0.45)",
      backdropFilter: "blur(1px)",
      zIndex: 1000
    },
    onClick: onClose
  }, /*#__PURE__*/React.createElement("div", _extends({
    role: "dialog",
    "aria-modal": "true",
    onClick: e => e.stopPropagation(),
    style: {
      width,
      maxWidth: "calc(100% - var(--dh-space-8))",
      background: "var(--dh-surface-card)",
      border: "1px solid var(--dh-border)",
      borderRadius: "var(--dh-radius-lg)",
      boxShadow: "var(--dh-shadow-lg)",
      color: "var(--dh-text-primary)",
      fontFamily: "var(--dh-font-sans)",
      overflow: "hidden",
      ...style
    }
  }, rest), /*#__PURE__*/React.createElement("div", {
    style: {
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      gap: "var(--dh-space-4)",
      padding: "var(--dh-space-5) var(--dh-space-6)",
      borderBottom: "1px solid var(--dh-border)"
    }
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      fontSize: "var(--dh-text-md)",
      fontWeight: "var(--dh-weight-semibold)"
    }
  }, title), onClose && /*#__PURE__*/React.createElement("button", {
    type: "button",
    onClick: onClose,
    "aria-label": "Close",
    style: {
      display: "inline-flex",
      width: 24,
      height: 24,
      alignItems: "center",
      justifyContent: "center",
      border: "none",
      background: "transparent",
      color: "var(--dh-text-secondary)",
      cursor: "pointer",
      borderRadius: "var(--dh-radius-md)"
    }
  }, /*#__PURE__*/React.createElement(__ds_scope.Icon, {
    name: "x",
    size: 16
  }))), /*#__PURE__*/React.createElement("div", {
    style: {
      padding: "var(--dh-space-6)",
      fontSize: "var(--dh-text-base)",
      lineHeight: "var(--dh-leading-normal)",
      color: "var(--dh-text-secondary)"
    }
  }, children), footer && /*#__PURE__*/React.createElement("div", {
    style: {
      display: "flex",
      justifyContent: "flex-end",
      gap: "var(--dh-space-3)",
      padding: "var(--dh-space-5) var(--dh-space-6)",
      borderTop: "1px solid var(--dh-border)",
      background: "var(--dh-surface-sunken)"
    }
  }, footer)));
}
Object.assign(__ds_scope, { Dialog });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/overlay/Dialog.jsx", error: String((e && e.message) || e) }); }

// components/overlay/Tooltip.jsx
try { (() => {
/**
 * Deephaven Tooltip — hover label on a trigger element.
 */
function Tooltip({
  label,
  placement = "top",
  children,
  style = {}
}) {
  const [show, setShow] = React.useState(false);
  const pos = {
    top: {
      bottom: "calc(100% + 6px)",
      left: "50%",
      transform: "translateX(-50%)"
    },
    bottom: {
      top: "calc(100% + 6px)",
      left: "50%",
      transform: "translateX(-50%)"
    },
    left: {
      right: "calc(100% + 6px)",
      top: "50%",
      transform: "translateY(-50%)"
    },
    right: {
      left: "calc(100% + 6px)",
      top: "50%",
      transform: "translateY(-50%)"
    }
  }[placement];
  return /*#__PURE__*/React.createElement("span", {
    style: {
      position: "relative",
      display: "inline-flex",
      ...style
    },
    onMouseEnter: () => setShow(true),
    onMouseLeave: () => setShow(false),
    onFocus: () => setShow(true),
    onBlur: () => setShow(false)
  }, children, show && /*#__PURE__*/React.createElement("span", {
    role: "tooltip",
    style: {
      position: "absolute",
      ...pos,
      zIndex: 1100,
      padding: "var(--dh-space-2) var(--dh-space-3)",
      background: "var(--dh-fg-100)",
      color: "var(--dh-bg-1100)",
      fontFamily: "var(--dh-font-sans)",
      fontSize: "var(--dh-text-sm)",
      fontWeight: "var(--dh-weight-medium)",
      lineHeight: 1.3,
      borderRadius: "var(--dh-radius-md)",
      boxShadow: "var(--dh-shadow-popover)",
      whiteSpace: "nowrap",
      pointerEvents: "none"
    }
  }, label));
}
Object.assign(__ds_scope, { Tooltip });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/overlay/Tooltip.jsx", error: String((e && e.message) || e) }); }

// ui_kits/console/AppShell.jsx
try { (() => {
// Deephaven IDE — app shell: top bar, left explorer, panel dock layout.
const {
  IconButton,
  Input,
  Icon,
  Badge,
  Tooltip
} = window.DeephavenDesignSystem_e929bc;
function TopBar() {
  return /*#__PURE__*/React.createElement("div", {
    style: {
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      height: 48,
      padding: "0 12px",
      background: "var(--dh-surface-card)",
      borderBottom: "1px solid var(--dh-border)",
      flex: "0 0 auto"
    }
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      display: "flex",
      alignItems: "center",
      gap: 14
    }
  }, /*#__PURE__*/React.createElement("img", {
    src: "../../assets/wordmark-on-dark.png",
    alt: "Deephaven",
    style: {
      height: 22
    }
  }), /*#__PURE__*/React.createElement("span", {
    style: {
      width: 1,
      height: 22,
      background: "var(--dh-border)"
    }
  }), /*#__PURE__*/React.createElement("span", {
    style: {
      fontFamily: "var(--dh-font-sans)",
      fontSize: 13,
      color: "var(--dh-text-secondary)"
    }
  }, "market-analytics"), /*#__PURE__*/React.createElement(Badge, {
    tone: "positive"
  }, "Worker ready")), /*#__PURE__*/React.createElement("div", {
    style: {
      display: "flex",
      alignItems: "center",
      gap: 8
    }
  }, /*#__PURE__*/React.createElement(Input, {
    placeholder: "Search commands\u2026",
    iconLeft: /*#__PURE__*/React.createElement(Icon, {
      name: "search",
      size: 14
    }),
    size: "sm",
    style: {
      width: 200
    }
  }), /*#__PURE__*/React.createElement(Tooltip, {
    label: "Notifications"
  }, /*#__PURE__*/React.createElement(IconButton, {
    name: "bell",
    label: "Notifications"
  })), /*#__PURE__*/React.createElement(Tooltip, {
    label: "Settings"
  }, /*#__PURE__*/React.createElement(IconButton, {
    name: "settings",
    label: "Settings"
  })), /*#__PURE__*/React.createElement("div", {
    style: {
      width: 28,
      height: 28,
      borderRadius: 999,
      background: "var(--dh-primary-800)",
      color: "#fff",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      fontSize: 12,
      fontWeight: 600,
      fontFamily: "var(--dh-font-sans)"
    }
  }, "DA")));
}
const TREE = [{
  icon: "table-2",
  label: "trades",
  tone: "var(--dh-secondary-600)",
  badge: "Live"
}, {
  icon: "table-2",
  label: "vwap",
  tone: "var(--dh-secondary-600)"
}, {
  icon: "table-2",
  label: "by_sym",
  tone: "var(--dh-secondary-600)"
}, {
  icon: "line-chart",
  label: "price_chart",
  tone: "var(--dh-info-700)"
}, {
  icon: "file-code",
  label: "market.py",
  tone: "var(--dh-text-muted)"
}, {
  icon: "file-code",
  label: "scratch.sql",
  tone: "var(--dh-text-muted)"
}];
function Explorer() {
  const [sel, setSel] = React.useState("trades");
  return /*#__PURE__*/React.createElement("div", {
    style: {
      width: 216,
      flex: "0 0 auto",
      background: "var(--dh-surface-card)",
      borderRight: "1px solid var(--dh-border)",
      display: "flex",
      flexDirection: "column"
    }
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      height: 40,
      padding: "0 8px 0 14px",
      borderBottom: "1px solid var(--dh-border)"
    }
  }, /*#__PURE__*/React.createElement("span", {
    style: {
      fontFamily: "var(--dh-font-sans)",
      fontSize: 11,
      fontWeight: 600,
      letterSpacing: "0.05em",
      textTransform: "uppercase",
      color: "var(--dh-text-muted)"
    }
  }, "Panels"), /*#__PURE__*/React.createElement(IconButton, {
    name: "plus",
    label: "New panel",
    size: "sm"
  })), /*#__PURE__*/React.createElement("div", {
    style: {
      padding: 6,
      overflow: "auto"
    }
  }, TREE.map(n => {
    const active = sel === n.label;
    return /*#__PURE__*/React.createElement("button", {
      key: n.label,
      onClick: () => setSel(n.label),
      style: {
        display: "flex",
        alignItems: "center",
        gap: 9,
        width: "100%",
        height: 30,
        padding: "0 8px",
        border: "none",
        borderRadius: "var(--dh-radius-md)",
        background: active ? "var(--dh-surface-hover)" : "transparent",
        color: "var(--dh-text-primary)",
        fontFamily: "var(--dh-font-mono)",
        fontSize: 12.5,
        cursor: "pointer",
        textAlign: "left"
      }
    }, /*#__PURE__*/React.createElement(Icon, {
      name: n.icon,
      size: 14,
      color: n.tone
    }), /*#__PURE__*/React.createElement("span", {
      style: {
        flex: 1
      }
    }, n.label), n.badge && /*#__PURE__*/React.createElement("span", {
      style: {
        fontSize: 9,
        fontWeight: 700,
        letterSpacing: "0.04em",
        textTransform: "uppercase",
        color: "var(--dh-status-positive)"
      }
    }, n.badge));
  })));
}
window.DHKit = window.DHKit || {};
window.DHKit.TopBar = TopBar;
window.DHKit.Explorer = Explorer;
})(); } catch (e) { __ds_ns.__errors.push({ path: "ui_kits/console/AppShell.jsx", error: String((e && e.message) || e) }); }

// ui_kits/console/Console.jsx
try { (() => {
// Deephaven IDE — code console. Editor with syntax-lit query + run log.
const {
  IconButton,
  Tabs
} = window.DeephavenDesignSystem_e929bc;
const CODE_LINES = [[["kw", "from"], ["p", " deephaven"], ["p", " import"], ["p", " agg"]], [], [["v", "trades"], ["p", " = db."], ["fn", "live_table"], ["p", "("], ["s", '"Market"'], ["p", ", "], ["s", '"Trades"'], ["p", ")"]], [["v", "vwap"], ["p", " = trades."], ["fn", "update"], ["p", "("], ["s", '"VWAP = cumSum(Price*Size)/cumSum(Size)"'], ["p", ")"]], [["v", "by_sym"], ["p", " = vwap."], ["fn", "agg_by"], ["p", "(["], ["fn", "agg.avg"], ["p", "("], ["s", '"AvgPx=Price"'], ["p", ")], "], ["s", '"Sym"'], ["p", ")"]]];
const COLORS = {
  kw: "var(--dh-info-700)",
  fn: "var(--dh-secondary-600)",
  s: "var(--dh-positive-700)",
  v: "var(--dh-warn-600)",
  p: "var(--dh-bg-1000)"
};
function Console() {
  const [tab, setTab] = React.useState("py");
  const [log, setLog] = React.useState([{
    t: "info",
    m: "Connected to worker · Python 3.11 · Deephaven Core 0.36"
  }, {
    t: "ok",
    m: "trades → table (streaming, 1,240,908 rows)"
  }, {
    t: "ok",
    m: "vwap → table (5 columns)"
  }]);
  function run() {
    setLog(l => [...l, {
      t: "run",
      m: "» executed selection (3 statements)"
    }, {
      t: "ok",
      m: "by_sym → table (2 columns · 10 groups) · 41ms"
    }]);
  }
  return /*#__PURE__*/React.createElement("div", {
    style: {
      display: "flex",
      flexDirection: "column",
      height: "100%",
      background: "var(--dh-surface-card)"
    }
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      padding: "0 8px 0 12px",
      height: 40,
      borderBottom: "1px solid var(--dh-border)",
      flex: "0 0 auto"
    }
  }, /*#__PURE__*/React.createElement(Tabs, {
    value: tab,
    onChange: setTab,
    tabs: [{
      value: "py",
      label: "market.py"
    }, {
      value: "sql",
      label: "scratch.sql"
    }],
    style: {
      border: "none"
    }
  }), /*#__PURE__*/React.createElement("div", {
    style: {
      display: "flex",
      gap: 2
    }
  }, /*#__PURE__*/React.createElement(IconButton, {
    name: "play",
    label: "Run all",
    onClick: run
  }), /*#__PURE__*/React.createElement(IconButton, {
    name: "rotate-cw",
    label: "Restart worker"
  }), /*#__PURE__*/React.createElement(IconButton, {
    name: "ellipsis",
    label: "More"
  }))), /*#__PURE__*/React.createElement("div", {
    style: {
      flex: 1,
      overflow: "auto",
      padding: "12px 0",
      fontFamily: "var(--dh-font-mono)",
      fontSize: 13,
      lineHeight: "20px"
    }
  }, CODE_LINES.map((line, i) => /*#__PURE__*/React.createElement("div", {
    key: i,
    style: {
      display: "flex",
      padding: "0 12px"
    }
  }, /*#__PURE__*/React.createElement("span", {
    style: {
      width: 28,
      color: "var(--dh-text-muted)",
      textAlign: "right",
      marginRight: 16,
      userSelect: "none",
      flex: "0 0 auto"
    }
  }, i + 1), /*#__PURE__*/React.createElement("span", {
    style: {
      whiteSpace: "pre"
    }
  }, line.map(([c, t], j) => /*#__PURE__*/React.createElement("span", {
    key: j,
    style: {
      color: COLORS[c]
    }
  }, t)))))), /*#__PURE__*/React.createElement("div", {
    style: {
      flex: "0 0 auto",
      maxHeight: 128,
      overflow: "auto",
      borderTop: "1px solid var(--dh-border)",
      background: "var(--dh-surface-sunken)",
      padding: "8px 0",
      fontFamily: "var(--dh-font-mono)",
      fontSize: 12,
      lineHeight: "18px"
    }
  }, log.map((l, i) => /*#__PURE__*/React.createElement("div", {
    key: i,
    style: {
      padding: "0 12px",
      color: l.t === "ok" ? "var(--dh-status-positive)" : l.t === "run" ? "var(--dh-secondary-600)" : "var(--dh-text-muted)"
    }
  }, l.m))));
}
window.DHKit = window.DHKit || {};
window.DHKit.Console = Console;
})(); } catch (e) { __ds_ns.__errors.push({ path: "ui_kits/console/Console.jsx", error: String((e && e.message) || e) }); }

// ui_kits/console/DataGrid.jsx
try { (() => {
// Deephaven IDE — data grid panel. Simplified visual recreation of the
// real-time table viewer: sticky header, monospace cells, numeric alignment,
// up/down tick coloring.
const {
  Badge
} = window.DeephavenDesignSystem_e929bc;
const COLS = ["Sym", "Timestamp", "Price", "Size", "VWAP", "Chg%"];
const SYMS = ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOG", "META", "AMD", "INTC", "NFLX"];
function seedRows() {
  const base = {
    AAPL: 227.1,
    MSFT: 418.3,
    NVDA: 121.4,
    TSLA: 246.9,
    AMZN: 184.2,
    GOOG: 172.5,
    META: 512.8,
    AMD: 158.7,
    INTC: 34.2,
    NFLX: 688.1
  };
  return SYMS.map((s, i) => {
    const price = base[s] + (Math.random() - 0.5) * 2;
    return {
      sym: s,
      ts: `09:3${i % 10}:${(10 + i).toString().padStart(2, "0")}.${(100 + i * 7) % 1000}`,
      price,
      size: (Math.floor(Math.random() * 20) + 1) * 100,
      vwap: price - (Math.random() - 0.5) * 0.4,
      chg: (Math.random() - 0.45) * 3
    };
  });
}
function DataGrid() {
  const [rows, setRows] = React.useState(seedRows);
  const [flash, setFlash] = React.useState({});
  React.useEffect(() => {
    const id = setInterval(() => {
      setRows(prev => {
        const i = Math.floor(Math.random() * prev.length);
        const next = prev.slice();
        const delta = (Math.random() - 0.5) * 1.2;
        const p = next[i].price + delta;
        next[i] = {
          ...next[i],
          price: p,
          vwap: p - (Math.random() - 0.5) * 0.4,
          size: (Math.floor(Math.random() * 20) + 1) * 100,
          chg: next[i].chg + delta / 10
        };
        setFlash({
          i,
          up: delta >= 0
        });
        return next;
      });
    }, 700);
    return () => clearInterval(id);
  }, []);
  React.useEffect(() => {
    if (flash.i == null) return;
    const t = setTimeout(() => setFlash({}), 400);
    return () => clearTimeout(t);
  }, [flash]);
  const cell = {
    padding: "0 12px",
    height: 26,
    display: "flex",
    alignItems: "center",
    fontFamily: "var(--dh-font-mono)",
    fontSize: 12,
    whiteSpace: "nowrap",
    borderBottom: "1px solid var(--dh-border)"
  };
  const num = {
    ...cell,
    justifyContent: "flex-end"
  };
  return /*#__PURE__*/React.createElement("div", {
    style: {
      display: "flex",
      flexDirection: "column",
      height: "100%",
      background: "var(--dh-surface-page)"
    }
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      display: "flex",
      alignItems: "center",
      gap: 10,
      padding: "0 12px",
      height: 34,
      borderBottom: "1px solid var(--dh-border)",
      flex: "0 0 auto"
    }
  }, /*#__PURE__*/React.createElement("span", {
    style: {
      fontFamily: "var(--dh-font-mono)",
      fontSize: 12,
      color: "var(--dh-text-primary)",
      fontWeight: 600
    }
  }, "trades"), /*#__PURE__*/React.createElement(Badge, {
    tone: "positive"
  }, "Live"), /*#__PURE__*/React.createElement("span", {
    style: {
      fontFamily: "var(--dh-font-mono)",
      fontSize: 11,
      color: "var(--dh-text-muted)"
    }
  }, rows.length.toLocaleString(), " rows \xB7 streaming")), /*#__PURE__*/React.createElement("div", {
    style: {
      flex: 1,
      overflow: "auto"
    }
  }, /*#__PURE__*/React.createElement("div", {
    style: {
      display: "grid",
      gridTemplateColumns: "70px 150px 1fr 80px 1fr 80px"
    }
  }, COLS.map(c => /*#__PURE__*/React.createElement("div", {
    key: c,
    style: {
      ...cell,
      position: "sticky",
      top: 0,
      background: "var(--dh-surface-raised)",
      color: "var(--dh-text-secondary)",
      fontWeight: 600,
      justifyContent: ["Price", "Size", "VWAP", "Chg%"].includes(c) ? "flex-end" : "flex-start",
      zIndex: 1
    }
  }, c)), rows.map((r, i) => {
    const isFlash = flash.i === i;
    const flashBg = isFlash ? flash.up ? "color-mix(in srgb, var(--dh-positive-600) 22%, transparent)" : "color-mix(in srgb, var(--dh-negative-600) 22%, transparent)" : "transparent";
    return /*#__PURE__*/React.createElement(React.Fragment, {
      key: r.sym
    }, /*#__PURE__*/React.createElement("div", {
      style: {
        ...cell,
        background: flashBg,
        color: "var(--dh-secondary-600)"
      }
    }, r.sym), /*#__PURE__*/React.createElement("div", {
      style: {
        ...cell,
        background: flashBg,
        color: "var(--dh-text-muted)"
      }
    }, r.ts), /*#__PURE__*/React.createElement("div", {
      style: {
        ...num,
        background: flashBg,
        color: "var(--dh-text-primary)"
      }
    }, r.price.toFixed(2)), /*#__PURE__*/React.createElement("div", {
      style: {
        ...num,
        background: flashBg,
        color: "var(--dh-text-secondary)"
      }
    }, r.size), /*#__PURE__*/React.createElement("div", {
      style: {
        ...num,
        background: flashBg,
        color: "var(--dh-text-secondary)"
      }
    }, r.vwap.toFixed(2)), /*#__PURE__*/React.createElement("div", {
      style: {
        ...num,
        background: flashBg,
        color: r.chg >= 0 ? "var(--dh-status-positive)" : "var(--dh-status-negative)"
      }
    }, r.chg >= 0 ? "+" : "", r.chg.toFixed(2), "%"));
  }))));
}
window.DHKit = window.DHKit || {};
window.DHKit.DataGrid = DataGrid;
})(); } catch (e) { __ds_ns.__errors.push({ path: "ui_kits/console/DataGrid.jsx", error: String((e && e.message) || e) }); }

__ds_ns.Button = __ds_scope.Button;

__ds_ns.IconButton = __ds_scope.IconButton;

__ds_ns.Badge = __ds_scope.Badge;

__ds_ns.Card = __ds_scope.Card;

__ds_ns.Icon = __ds_scope.Icon;

__ds_ns.Tag = __ds_scope.Tag;

__ds_ns.Checkbox = __ds_scope.Checkbox;

__ds_ns.Input = __ds_scope.Input;

__ds_ns.Radio = __ds_scope.Radio;

__ds_ns.Select = __ds_scope.Select;

__ds_ns.Switch = __ds_scope.Switch;

__ds_ns.Tabs = __ds_scope.Tabs;

__ds_ns.Dialog = __ds_scope.Dialog;

__ds_ns.Tooltip = __ds_scope.Tooltip;

})();
