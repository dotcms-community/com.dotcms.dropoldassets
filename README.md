# README

This plugin Force Runs DropOldAssets.  It can be configured to run on a scheduled basis by editing the plugin.properties file

## How to build this example

To build the JAR, run the following Maven command: 
```sh
mvn clean install
```

This will generate the plugin JAR in the `target` directory.

## How to install this bundle

* **To install this bundle:**

  Copy the bundle JAR file inside the Felix OSGI container (`dotCMS/felix/load`).

  OR

  Upload the bundle JAR file using the dotCMS UI (`CMS Admin -> Plugins -> Upload Plugin`).

* **To uninstall this bundle:**

  Remove the bundle JAR file from the Felix OSGI container (`dotCMS/felix/load`).

  OR

  Undeploy the bundle JAR using the dotCMS UI (`CMS Admin -> Plugins -> Undeploy`).

## How to create a bundle plugin for Annotation Framework based on AOP  

In order to create this OSGI plugin, Maven is configured to generate the `META-INF/MANIFEST.MF` file automatically. If needed, you can customize the configuration in the `pom.xml`.

Below is a description of the required fields in the `MANIFEST.MF` and how they are configured in a `pom.xml`:

> **Bundle-Name:** The name of your bundle  
> **Bundle-SymbolicName:** A short and unique name for the bundle  
> **Bundle-Vendor:** The vendor of the bundle (example: dotCMS)  
> **Bundle-Description:** A brief description of the bundle  
> **Bundle-DocURL:** URL for the bundle documentation  
> **Bundle-Activator:** Package and name of your Activator class (example: com.dotmarketing.osgi.actionlet.Activator)  
> **Export-Package:** Declares the packages that are visible outside the plugin. Any package not declared here has visibility only within the bundle.  
> **Import-Package:** This is a comma-separated list of the names of packages to import. This list must include the packages that you are using inside your OSGI bundle plugin and are exported and exposed by the dotCMS runtime.

These fields are configured in the `pom.xml` as follows:

```xml
<plugin>
    <groupId>org.apache.felix</groupId>
    <artifactId>maven-bundle-plugin</artifactId>
    <version>5.1.9</version>
    <extensions>true</extensions>
    <configuration>
        <instructions>
            <Bundle-Name>Your Bundle Name</Bundle-Name>
            <Bundle-SymbolicName>com.example.yourbundle</Bundle-SymbolicName>
            <Bundle-Vendor>dotCMS</Bundle-Vendor>
            <Bundle-Description>dotCMS - OSGI Actionlet example</Bundle-Description>
            <Bundle-DocURL>https://dotcms.com/</Bundle-DocURL>
            <Bundle-Activator>com.dotmarketing.osgi.actionlet.Activator</Bundle-Activator>
            <Export-Package>com.example.yourbundle.package</Export-Package>
            <Import-Package>*</Import-Package>
        </instructions>
    </configuration>
</plugin>
```

## Beware (!)

In order to work inside the Apache Felix OSGI runtime, the import and export directives must be bidirectional:

* **Exported Packages**

  The dotCMS must declare the set of packages that will be available to the OSGI plugins by updating the file: `dotCMS/WEB-INF/felix/osgi-extra.conf`.
  This can also be configured using the dotCMS UI (`CMS Admin -> Plugins -> Exported Packages`).

  Only after the exported packages are defined in this list, can a plugin import the packages to use them inside the OSGI bundle.

* **Fragment (Deprecated)**

  Previously, a bundle fragment was used to make its contents available to other bundles by exporting 3rd party libraries from dotCMS. Fragments do not participate in the lifecycle of the bundle and therefore cannot have a Bundle-Activator. As this is no longer required, this section is deprecated.

---
## Components

### com.dotmarketing.portlets.folders.model.Folder

Copy of the original *com.dotmarketing.portlets.folders.model.Folder* that lives inside dotCMS but with small changes (Just logging code) inside the `getPath()` method in order to demonstrate we can override a dotCMS class with our implementation.

### Activator

This bundle activator extends from *com.dotmarketing.osgi.GenericBundleActivator* and implements `BundleActivator.start()`.

* PLEASE note the `initializeServices( context )` call, this call is MANDATORY (!) as it will allow us to share resources between the bundle and the host container (dotCMS) and override classes.

---
## Testing

For our redefinition of the class *com.dotmarketing.portlets.folders.model.Folder* we modified the body of the `getPath()` method, we added some debugging lines in order to prove it is working.
The *Folder* class is call it by our activator (*com.dotmarketing.osgi.override.Activator*) which prove the OSGI bundle is using our redefinition of the Folder class, but in order to demonstrate the dotCMS context is using it too go to:
*dotCMS Admin -> Site Browser -> Select any folder on the tree -> Add Folder*.

The *Add Folder* uses the *Folder* class and if you deployed your OSGI plugin the debugging code we added should be visible on the logs.

---

## Limitations (!)

There are limitations on the hot deploy functionality for the OSGI Override plugin, there are some rules for the code you can modify on the redefined classes in order to see those changes when you upload the plugin.

In order to support the overriding of a class inside ours OSGI plugins we use the java hot swapping, it allows to redefine classes, unfortunately, this redefinition is limited only to changing method bodies:

> The redefinition may change method bodies, the constant pool and attributes. The redefinition must not add, remove or rename fields or methods, change the signatures of methods, or change inheritance.

As long as you don't add, remove or change methods (ONLY the methods bodies) for your redefined classes you will have an OSGI plugin that will reflect you changes when a deploy is done.
