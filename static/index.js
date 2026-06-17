const RENDER_COMPLETE_EVENT = "xwork:render:complete"

const fetchComponent = (name) => {
    return fetch(`/components/${name}.html`).then(res => res.text())
}

const resolveComponents = async () => {
    const promises = Array.from(document.querySelectorAll("[xwork-component]").values()).map(async template => {
        const componentName = template.getAttribute("xwork-component")

        template.outerHTML = await fetchComponent(componentName)
    })

    await Promise.all(promises)

    document.dispatchEvent(new Event(RENDER_COMPLETE_EVENT))
}

document.addEventListener("DOMContentLoaded", resolveComponents)

document.addEventListener(RENDER_COMPLETE_EVENT, () => {
    document.querySelectorAll(".nav-link").forEach(navLink => {
        if (navLink.href === window.location.href) {
            navLink.classList.add("active")
        }
    })
})
