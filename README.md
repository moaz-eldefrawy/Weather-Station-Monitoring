# Weather-Station-Monitoring

# React Tilt

_👀 Easily apply tilt hover effect on React components_

[![](misc/demo.gif)](https://mkosir.github.io/react-parallax-tilt/?path=/story/react-parallax-tilt--glare-effect)

## [Demo 💥](https://mkosir.github.io/react-parallax-tilt)

## Install

```bash
npm install react-parallax-tilt
```

## Features

- Lightweight (≈3kB), zero dependencies 📦
- Works with React v15 onwards
- Supports **mouse** and **touch** events
- Support for device tilting (**gyroscope**)
- **Glare** effect 🌟 with custom props (color, position,...) [🔗demo](https://mkosir.github.io/react-parallax-tilt/?path=/story/react-parallax-tilt--parallax-effect-glare-scale)
- Events to keep track of component values 📐 (tilt, glare, mousemove,...) [🔗demo](https://mkosir.github.io/react-parallax-tilt/?path=/story/react-parallax-tilt--event-params)
- Many effects that can be easily applied:
  - **scale** on hover [🔗demo](https://mkosir.github.io/react-parallax-tilt/?path=/story/react-parallax-tilt--scale)
  - **disable** x/y axis [🔗demo](https://mkosir.github.io/react-parallax-tilt/?path=/story/react-parallax-tilt--tilt-disable-axis)
  - **flip** component vertically/horizontally [🔗demo](https://mkosir.github.io/react-parallax-tilt/?path=/story/react-parallax-tilt--flip-vh)
  - tilt hover effect on the **whole window** [🔗demo](https://mkosir.github.io/react-parallax-tilt/?path=/story/react-parallax-tilt--track-on-window)
  - tilt component with custom **manual input** 🕹 (joystick, slider etc.) [🔗demo](https://mkosir.github.io/react-parallax-tilt/?path=/story/react-parallax-tilt--tilt-manual-input)
  - **parallax** effect on overlaid images [🔗demo](https://mkosir.github.io/react-parallax-tilt/?path=/story/react-parallax-tilt--parallax-effect-img)

<details>
<summary>Using device orientation on iOS 13+</summary>

Apple decided turning device motion and orientation off by default since iOS 12.2.  
With iOS 13+ permission API can be used to gain access to device orientation event.

When using gyroscope feature:

```jsx
<Tilt gyroscope={true}>
  <h1>React Parallax Tilt 👀</h1>
</Tilt>
```

it will present a permission dialog prompting the user to allow motion and orientation access at domain level:  
![](misc/device_orientation.jpg)

Note that user needs to take some action (like tapping a button) to be able to display the dialog (invoking dialog on page load is not possible).

</details>

## Development

_Easily set up a local development environment!_

Build project and start storybook on [localhost](http://localhost:9009):

- clone
- `npm install`
- `npm start`

```



```
