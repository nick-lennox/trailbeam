# Releasing the Java jar to Maven Central

Maintainers only. CI never publishes, and neither do agents: every step needs a key or an account that only a
maintainer holds. A release on Maven Central can never be deleted or changed, so the pom (its `url`, `scm`,
license and developers) must be right before the first one.

## Once, before the first release

1. **Central Portal account and namespace.** Sign in at <https://central.sonatype.com>, open *Namespaces* and add
   `app.trailbeam`. The Portal shows a verification key. Add it as a DNS TXT record on `trailbeam.app` (host
   `trailbeam.app`, type `TXT`, value: the key), then press *Verify* on the Portal.
2. **Portal token.** Under *Account*, generate a user token and put it in `~/.m2/settings.xml`:

   ```xml
   <settings>
     <servers>
       <server>
         <id>central</id>
         <username><!-- token username --></username>
         <password><!-- token password --></password>
       </server>
     </servers>
   </settings>
   ```

3. **GPG key.** Make a signing key (`gpg --full-generate-key`) and publish its public half where Central looks,
   for example `gpg --keyserver keys.openpgp.org --send-keys <KEY_ID>`.

## Each release

Run from `java/`:

1. Set a release version. The Portal's release upload doesn't take `-SNAPSHOT` versions:
   `mvn versions:set -DnewVersion=0.1.0 -DgenerateBackupPoms=false`
2. Build, sign and upload: `mvn clean deploy -Prelease`. GPG asks for the key's passphrase. Automatic
   publishing is off, so the upload is validated and then waits on the Portal.
3. On the Portal, under *Deployments*, check the files (the jar, `-sources.jar`, `-javadoc.jar`, the pom, and an
   `.asc` signature for each), then press *Publish*, or *Drop* to throw the upload away.
4. Commit the version, tag it (`java-v0.1.0`), and set the next snapshot version
   (`mvn versions:set -DnewVersion=0.1.1-SNAPSHOT -DgenerateBackupPoms=false`).
