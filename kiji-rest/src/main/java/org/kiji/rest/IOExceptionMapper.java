/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.rest;

import java.io.IOException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.sun.jersey.core.spi.factory.ResponseBuilderImpl;

import org.kiji.rest.representations.ExceptionWrapper;

/**
 * A somewhat catch-all mapper to map RuntimeExceptions to something readable. Most other
 * exceptions are mapped via the WebApplicationException.
 *
 */

@Provider
public class IOExceptionMapper implements ExceptionMapper<IOException> {

  /**
   * {@inheritDoc}
   */
  @Override
  public Response toResponse(IOException thrownException) {
    ResponseBuilder builder = new ResponseBuilderImpl();
    builder.type(MediaType.APPLICATION_JSON);
    Status status = Status.INTERNAL_SERVER_ERROR;
    builder.status(status);
    builder.entity(new ExceptionWrapper(status, thrownException));
    return builder.build();
  }
}
