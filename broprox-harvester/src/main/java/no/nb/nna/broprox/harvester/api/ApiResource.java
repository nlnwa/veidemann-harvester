/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.broprox.harvester.api;

import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import no.nb.nna.broprox.harvester.Harvester;
import no.nb.nna.broprox.harvester.browsercontroller.BrowserController;

/**
 *
 */
@Path("snapshot")
public class ApiResource {

    final String browserHost = Harvester.getSettings().getBrowserHost();

    final int browserPort = Harvester.getSettings().getBrowserPort();

    @GET
    @Produces("image/png")
    public byte[] getHello(@QueryParam("h") int height, @QueryParam("w") int width,
            @QueryParam("scaledHeight") @DefaultValue("0") int scaledHeight, @QueryParam("url") String url) {
        try (BrowserController ab = new BrowserController(browserHost, browserPort);) {
            byte[] imgArray = ab.render(url, width, height, 10000, 1000);
            return scale(imgArray, scaledHeight);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new WebApplicationException(ex);
        }
    }

    byte[] scale(byte[] in, int scaledHeight) throws IOException {
        BufferedImage img = ImageIO.read(new ByteArrayInputStream(in));

        if (scaledHeight == 0) {
            return in;
        }

        int w = img.getWidth();
        int h = img.getHeight();
        double factor = (double) scaledHeight / h;
        int sWidth = (int) (w * factor);
        int sHeight = (int) (h * factor);

        BufferedImage scaledImg = new BufferedImage(sWidth, sHeight, img.getType());
        AffineTransform t = AffineTransform.getScaleInstance(factor, factor);
        Graphics2D g2d = scaledImg.createGraphics();
        g2d.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
        g2d.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY);
        g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        g2d.drawImage(img, t, null);
        g2d.dispose();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ImageIO.write(scaledImg, "png", out);
        return out.toByteArray();
    }

}
